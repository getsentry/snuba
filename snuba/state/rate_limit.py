from __future__ import annotations

import logging
import sys
import time
import uuid
from collections import ChainMap, namedtuple
from contextlib import AbstractContextManager, ExitStack, contextmanager
from dataclasses import dataclass
from types import TracebackType
from typing import Any
from typing import ChainMap as TypingChainMap
from typing import Iterator, MutableMapping, Optional, Sequence, Type

from snuba import environment, state
from snuba.redis import RedisClientKey, get_redis_client
from snuba.state import get_configs, set_config
from snuba.utils.metrics.wrapper import MetricsWrapper
from snuba.utils.serializable_exception import SerializableException

logger = logging.getLogger("snuba.state.rate_limit")

ORGANIZATION_RATE_LIMIT_NAME = "organization"
PROJECT_RATE_LIMIT_NAME = "project"
PROJECT_REFERRER_RATE_LIMIT_NAME = "project_referrer"
REFERRER_RATE_LIMIT_NAME = "referrer"
TABLE_RATE_LIMIT_NAME = "table"

metrics = MetricsWrapper(environment.metrics, "api")

rds = get_redis_client(RedisClientKey.RATE_LIMITER)


def get_rate_limit_config(
    per_second: tuple[str, float | None], concurrent: tuple[str, int | None]
) -> tuple[Any, Any]:
    """
    This function to encapsulate how rate limit keys are fetched from Redis, since
    that is conceptually a different process from fetching normal config keys.

    It currently also contains logic to read from both the new namespace and the old one,
    until everything is properly migrated.

    First step: read from the old and new namespace. If the new namespace is different from
    the old, copy the value from old to new. Return the values from the old namespace.
    """
    ps_name, per_second_default = per_second
    ct_name, concurrent_default = concurrent

    # Dual read
    # if the value in new doesn't match value from old, copy over
    old_per_second, old_concurrent = get_configs([(ps_name, None), (ct_name, None)])
    new_per_second, new_concurrent = get_configs(
        [(ps_name, None), (ct_name, None)], config_key=state.rate_limit_config_key
    )

    # This handles deletes as well, since writing None deletes the key
    if old_per_second != new_per_second:
        set_rate_limit_config(ps_name, old_per_second, copy=False)
    if old_concurrent != new_concurrent:
        set_rate_limit_config(ct_name, old_concurrent, copy=False)

    found_per_second = (
        old_per_second if old_per_second is not None else per_second_default
    )
    found_concurrent = (
        old_concurrent if old_concurrent is not None else concurrent_default
    )

    return (found_per_second, found_concurrent)


def set_rate_limit_config(
    bucket: str, value: float | int | None, copy: bool = True
) -> None:
    """
    This function to encapsulate how rate limit keys are set in Redis, since
    that is conceptually a different process from writing normal config keys.

    Generally, if this function is called, data should be written to both the old
    and new keys while the migration is happening. However, there are some instances
    when only the new key needs to be written, so there is a flag for that case.
    """
    set_config(
        bucket,
        value,
        config_key=state.rate_limit_config_key,
    )
    if copy:
        set_config(bucket, value)


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
    ) -> MutableMapping[str, float]:
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


def _get_bucket_key(prefix: str, bucket: str, shard_id: int) -> str:
    shard_suffix = ""
    if shard_id > 0:
        # special case shard 0 so that it is backwards-compatible with the
        # previous version of this rate limiter that did not have a concept of
        # sharding.
        shard_suffix = f":shard-{shard_id}"

    return "{}{}{}".format(prefix, bucket, shard_suffix)


def rate_limit_start_request(
    rate_limit_params: RateLimitParameters,
    query_id: str,
    rate_history_sec,
    rate_limit_shard_factor,
) -> RateLimitStats:
    now = time.time()

    # Compute the set shard to which we should add and remove the query_id
    bucket_shard = hash(query_id) % rate_limit_shard_factor
    query_bucket = _get_bucket_key(
        state.ratelimit_prefix, rate_limit_params.bucket, bucket_shard
    )

    pipe = rds.pipeline(transaction=False)
    # cleanup old query timestamps past our retention window
    #
    # it is fine to only perform this cleanup for the shard of the current
    # query, because on average there will be many other queries that hit other
    # shards and perform cleanup there
    pipe.zremrangebyscore(query_bucket, "-inf", "({:f}".format(now - rate_history_sec))

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
    pipe.zadd(query_bucket, {query_id: now + state.max_query_duration_s})

    # bump the expiration date of the entire set so that it roughly aligns with
    # the expiration date of the latest item.
    #
    # we do this in order to avoid leaking redis sets in the event that two
    # things occur at the same time:
    # 1. a bucket stops receiving requests (this can happen if a bucket
    #    corresponds to a deleted project id)
    # 2. a previous request to the same bucket was killed off so that the set
    #    has a dangling item (ie. a process was killed)
    #
    # the TTL is calculated as such:
    #
    # * in the previous zadd command, the last item is inserted with timestamp
    #   `now + max_query_duration_s`.
    # * the next query's zremrangebyscore would remove this item on `now +
    #   max_query_duration_s + rate_history_s` at the earliest.
    # * add +1 to account for rounding errors when casting to int
    pipe.expire(query_bucket, int(state.max_query_duration_s + rate_history_sec + 1))

    if rate_limit_params.per_second_limit is not None:
        # count queries that have finished for the per-second rate
        for shard_i in range(rate_limit_shard_factor):
            bucket = _get_bucket_key(
                state.ratelimit_prefix, rate_limit_params.bucket, shard_i
            )
            pipe.zcount(bucket, now - state.rate_lookback_s, now)

    if rate_limit_params.concurrent_limit is not None:
        # count the amount queries in the "future" which tells us the amount
        # of concurrent queries
        for shard_i in range(rate_limit_shard_factor):
            bucket = _get_bucket_key(
                state.ratelimit_prefix, rate_limit_params.bucket, shard_i
            )
            pipe.zcount(bucket, "({:f}".format(now), "+inf")

    try:
        results = pipe.execute()
        pipe_results = iter(results)

        # skip zremrangebyscore, zadd and expire
        next(pipe_results)
        next(pipe_results)
        next(pipe_results)

        if rate_limit_params.per_second_limit is not None:
            historical = sum(next(pipe_results) for _ in range(rate_limit_shard_factor))
        else:
            historical = 0

        if rate_limit_params.concurrent_limit is not None:
            concurrent = sum(next(pipe_results) for _ in range(rate_limit_shard_factor))
        else:
            concurrent = 0
    except Exception as ex:
        # if something goes wrong, we don't want to block the request,
        # set the values such that they pass under any limit
        logger.exception(ex)
        return RateLimitStats(rate=-1, concurrent=-1)

    per_second = historical / float(state.rate_lookback_s)

    stats = RateLimitStats(rate=per_second, concurrent=concurrent)
    return stats


def rate_limit_finish_request(
    rate_limit_params: RateLimitParameters,
    query_id: str,
    rate_limit_shard_factor: int,
    was_rate_limited: bool,
) -> None:
    bucket_shard = hash(query_id) % rate_limit_shard_factor
    query_bucket = _get_bucket_key(
        state.ratelimit_prefix, rate_limit_params.bucket, bucket_shard
    )
    if was_rate_limited:
        try:
            rds.zrem(query_bucket, query_id)  # not allowed / not counted
        except Exception as ex:
            logger.exception(ex)
    else:
        try:
            # return the query to its start time, if the query_id was actually added.
            rds.zincrby(query_bucket, -float(state.max_query_duration_s), query_id)
        except Exception as ex:
            logger.exception(ex)


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
    (bypass_rate_limit, rate_history_s, rate_limit_shard_factor,) = state.get_configs(
        [
            # bool (0/1) flag to disable rate limits altogether
            ("bypass_rate_limit", 0),
            # number of seconds the timestamps are kept
            ("rate_history_sec", 3600),
            # number of shards that each redis set is supposed to have.
            # increasing this value multiplies the number of redis keys by that
            # factor, and (on average) reduces the size of each redis set
            ("rate_limit_shard_factor", 1),
        ]
    )
    assert isinstance(rate_history_s, (int, float))
    assert isinstance(rate_limit_shard_factor, int)
    assert rate_limit_shard_factor > 0

    if bypass_rate_limit == 1:
        yield None
        return

    query_id_uuid = uuid.uuid4()
    query_id = str(query_id_uuid)

    rate_limit_stats = rate_limit_start_request(
        rate_limit_params, query_id, rate_history_s, rate_limit_shard_factor
    )

    rate_limit_name = rate_limit_params.rate_limit_name

    Reason = namedtuple("Reason", "scope name val limit")
    reasons = [
        Reason(
            rate_limit_name,
            "concurrent",
            rate_limit_stats.concurrent,
            rate_limit_params.concurrent_limit,
        ),
        Reason(
            rate_limit_name,
            "per-second",
            rate_limit_stats.rate,
            rate_limit_params.per_second_limit,
        ),
    ]
    reason = next((r for r in reasons if r.limit is not None and r.val > r.limit), None)
    if reason:
        rate_limit_finish_request(
            rate_limit_params, query_id, rate_limit_shard_factor, True
        )

        raise RateLimitExceeded(
            "{r.scope} {r.name} of {r.val:.0f} exceeds limit of {r.limit:.0f}".format(
                r=reason
            ),
            scope=reason.scope,
            name=reason.name,
        )

    try:
        yield rate_limit_stats
        _, err, _ = sys.exc_info()
        if isinstance(err, RateLimitExceeded):
            # If another rate limiter throws an exception, it won't be propagated
            # through this context. So check for the exception explicitly.
            # If another rate limit was hit, we don't want to count this query
            # against this limit.
            rate_limit_finish_request(
                rate_limit_params, query_id, rate_limit_shard_factor, True
            )
    finally:
        rate_limit_finish_request(
            rate_limit_params, query_id, rate_limit_shard_factor, False
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
    if scope == TABLE_RATE_LIMIT_NAME:
        tags = {"scope": scope, "type": name, "table": rate_limit_param.bucket}
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
