import logging
import sys
import time
import uuid
from collections import ChainMap, namedtuple
from contextlib import AbstractContextManager, ExitStack, contextmanager
from dataclasses import dataclass
from types import TracebackType
from typing import ChainMap as TypingChainMap
from typing import Iterator, Mapping, MutableMapping, Optional, Sequence, Type

from snuba import environment, state
from snuba.redis import redis_client as rds
from snuba.utils.metrics.wrapper import MetricsWrapper
from snuba.utils.serializable_exception import SerializableException

logger = logging.getLogger("snuba.state.rate_limit")

ORGANIZATION_RATE_LIMIT_NAME = "organization"
PROJECT_RATE_LIMIT_NAME = "project"
PROJECT_REFERRER_RATE_LIMIT_NAME = "project_referrer"
REFERRER_RATE_LIMIT_NAME = "referrer"
GLOBAL_RATE_LIMIT_NAME = "global"
TABLE_RATE_LIMIT_NAME = "table"

metrics = MetricsWrapper(environment.metrics, "api")


@dataclass(frozen=True)
class RateLimitParameters:
    """
    The configuration object which defines all the needed properties to create
    a rate limit.
    """

    rate_limit_name: str
    bucket: str
    per_second_limit: Optional[float]
    concurrent_limit: Optional[int]


class RateLimitExceeded(SerializableException):
    """
    Exception thrown when the rate limit is exceeded. scope and name are
    additional parameters which are provided when the exception is raised.
    """


@dataclass(frozen=True)
class RateLimitStats:
    """
    The stats returned after the rate limit is run to tell the caller about the current
    rate and number of concurrent requests.
    """

    rate: float
    concurrent: int


class RateLimitStatsContainer:
    """
    A container to collect stats for all the rate limits that have been run.
    """

    def __init__(self) -> None:
        self.__stats: MutableMapping[str, RateLimitStats] = {}

    def add_stats(self, rate_limit_name: str, rate_limit_stats: RateLimitStats) -> None:
        self.__stats[rate_limit_name] = rate_limit_stats

    def get_stats(self, rate_limit_name: str) -> Optional[RateLimitStats]:
        return self.__stats.get(rate_limit_name)

    def __format_single_dict(
        self, name: str, stats: RateLimitStats
    ) -> Mapping[str, float]:
        return {
            f"{name}_rate": stats.rate,
            f"{name}_concurrent": stats.concurrent,
        }

    def to_dict(self) -> TypingChainMap[str, float]:
        """
        Converts the internal representation into a mapping so that it can be added to
        the stats that are returned in the response body
        """
        grouped_stats = [
            self.__format_single_dict(name, rate_limit)
            for name, rate_limit in self.__stats.items()
        ]
        return ChainMap(*grouped_stats)


@contextmanager
def rate_limit(
    rate_limit_params: RateLimitParameters,
) -> Iterator[Optional[RateLimitStats]]:
    """
    A context manager for rate limiting that allows for limiting based on:
        * a rolling-window per-second rate
        * the number of queries concurrently running.

    It uses one redis sorted set to keep track of both of these limits
    The following mapping is kept in redis:

        bucket: SortedSet([(timestamp1, query_id1), (timestamp2, query_id2) ...])


    Queries are thrown ahead in time when they start so we can count them
    as concurrent, and thrown back to their start time once they finish so
    we can count them towards the historical rate. See the comments for
    an example.

               time >>----->
    +-----------------------------+--------------------------------+
    | historical query window     | currently executing queries    |
    +-----------------------------+--------------------------------+
                                  ^
                                 now
    """

    bucket = "{}{}".format(state.ratelimit_prefix, rate_limit_params.bucket)
    query_id = str(uuid.uuid4())

    now = time.time()
    bypass_rate_limit, rate_history_s = state.get_configs(
        [("bypass_rate_limit", 0), ("rate_history_sec", 3600)]
        #                               ^ number of seconds the timestamps are kept
    )
    assert isinstance(rate_history_s, (int, float))

    if bypass_rate_limit == 1:
        yield None
        return

    pipe = rds.pipeline(transaction=False)
    # cleanup old query timestamps past our retention window
    stale_queries = pipe.zremrangebyscore(
        bucket, "-inf", "({:f}".format(now - rate_history_s)
    )
    metrics.increment("rate_limit.stale", stale_queries, tags={"bucket": bucket})

    # Now for the tricky bit:
    # ======================
    # The query's *deadline* is added to the sorted set of timestamps, therefore
    # labeling its execution as in the future.

    # All queries with timestamps in the future are considered to be executing *right now*
    # Example:

    # now = 100
    # max_query_duration_s = 30
    # rate_lookback_s = 10
    # sorted_set (timestamps only for clarity) = [91, 94, 97, 103, 105, 130]

    # EXPLANATION:
    # ===========

    # queries that have finished running
    # (in this example there are 3 queries in the last 10 seconds
    #  thus the per second rate is 3/10 = 0.3)
    #      |
    #      v
    #  -----------              v--- the current query, vaulted into the future
    #  [91, 94, 97, 103, 105, 130]
    #               -------------- < - queries currently running
    #                                (how many queries are
    #                                   running concurrently; in this case 3)
    #              ^
    #              | current time
    pipe.zadd(bucket, {query_id: now + state.max_query_duration_s})
    if rate_limit_params.per_second_limit is None:
        pipe.exists("nosuchkey")  # no-op if we don't need per-second
    else:
        # count queries that have finished for the per-second rate
        pipe.zcount(bucket, now - state.rate_lookback_s, now)
    if rate_limit_params.concurrent_limit is None:
        pipe.exists("nosuchkey")  # no-op if we don't need concurrent
    else:
        # count the amount queries in the "future" which tells us the amount
        # of concurrent queries
        pipe.zcount(bucket, "({:f}".format(now), "+inf")

    try:
        _, _, historical, concurrent = pipe.execute()
        historical = int(historical)
        concurrent = int(concurrent)
    except Exception as ex:
        logger.exception(ex)
        yield None  # fail open if redis is having issues
        return

    per_second = historical / float(state.rate_lookback_s)

    stats = RateLimitStats(rate=per_second, concurrent=concurrent)

    rate_limit_name = rate_limit_params.rate_limit_name

    Reason = namedtuple("Reason", "scope name val limit")
    reasons = [
        Reason(
            rate_limit_name,
            "concurrent",
            concurrent,
            rate_limit_params.concurrent_limit,
        ),
        Reason(
            rate_limit_name,
            "per-second",
            per_second,
            rate_limit_params.per_second_limit,
        ),
    ]
    reason = next((r for r in reasons if r.limit is not None and r.val > r.limit), None)
    if reason:
        try:
            # Remove the query from the sorted set
            # because we rate limited it. It shouldn't count towards
            # rate limiting future queries in this bucket.
            rds.zrem(bucket, query_id)
        except Exception as ex:
            logger.exception(ex)

        raise RateLimitExceeded(
            "{r.scope} {r.name} of {r.val:.0f} exceeds limit of {r.limit:.0f}".format(
                r=reason
            ),
            scope=reason.scope,
            name=reason.name,
        )

    rate_limited = False
    try:
        yield stats
        _, err, _ = sys.exc_info()
        if isinstance(err, RateLimitExceeded):
            # If another rate limiter throws an exception, it won't be propagated
            # through this context. So check for the exception explicitly.
            # If another rate limit was hit, we don't want to count this query
            # against this limit.
            try:
                rds.zrem(bucket, query_id)  # not allowed / not counted
                rate_limited = True
            except Exception as ex:
                logger.exception(ex)
    finally:
        try:
            # return the query to its start time, if the query_id was actually added.
            if not rate_limited:
                rds.zincrby(bucket, -float(state.max_query_duration_s), query_id)
        except Exception as ex:
            logger.exception(ex)


def get_global_rate_limit_params() -> RateLimitParameters:
    """
    Returns the configuration object for the global rate limit
    """

    (per_second, concurr) = state.get_configs(
        [("global_per_second_limit", None), ("global_concurrent_limit", 1000)]
    )

    return RateLimitParameters(
        rate_limit_name=GLOBAL_RATE_LIMIT_NAME,
        bucket="global",
        per_second_limit=per_second,
        concurrent_limit=concurr,
    )


def _record_metrics(
    exc: RateLimitExceeded, rate_limit_param: RateLimitParameters
) -> None:
    """
    Record rate limit metrics if needed.

    We only record the metrics for global and table rate limits since
    those indicate capacity of clickhouse clusters.

    We get the scope and name from the message of RateLimitExceeded
    exception since we want to know whether we exceeded the concurrent
    or per second limit.
    """
    scope = str(exc.extra_data.get("scope", ""))
    name = str(exc.extra_data.get("name", ""))
    if scope == GLOBAL_RATE_LIMIT_NAME or scope == TABLE_RATE_LIMIT_NAME:
        tags = {"scope": scope, "type": name}
        if scope == TABLE_RATE_LIMIT_NAME:
            tags["table"] = rate_limit_param.bucket

        metrics.increment("rate-limited", tags=tags)


class RateLimitAggregator(AbstractContextManager):  # type: ignore
    """
    Runs the rate limits provided by the `rate_limit_params` configuration object.

    It runs the rate limits in the order described by `rate_limit_params`.
    """

    def __init__(self, rate_limit_params: Sequence[RateLimitParameters]) -> None:
        self.rate_limit_params = rate_limit_params
        self.stack = ExitStack()

    def __enter__(self) -> RateLimitStatsContainer:
        stats = RateLimitStatsContainer()

        for rate_limit_param in self.rate_limit_params:
            try:
                child_stats = self.stack.enter_context(rate_limit(rate_limit_param))
                if child_stats:
                    stats.add_stats(rate_limit_param.rate_limit_name, child_stats)
            except RateLimitExceeded as e:
                # If an exception occurs in one of the rate limiters, the __exit__ callbacks are not
                # called since the error happened in the __enter__ method and not in the context
                # block itself. In the case that one of the rate limiters caught a limit, we need
                # these exit functions to be called so we can roll back any limits that were set
                # earlier in the stack.
                self.__exit__(*sys.exc_info())
                _record_metrics(e, rate_limit_param)
                raise e

        return stats

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        self.stack.pop_all().close()
