from collections import namedtuple
from contextlib import contextmanager, AbstractContextManager, ExitStack
from dataclasses import dataclass
import logging
import time
from types import TracebackType
from typing import (
    Iterator,
    Mapping,
    MutableMapping,
    Optional,
    Sequence,
    Type
)
import uuid

from snuba import state

logger = logging.getLogger('snuba.state.rate_limit')


@dataclass(frozen=True)
class RateLimitParameters:
    """
    The configuration object which defines all the needed properties to create
    a rate limit.
    """

    rate_limit_name: str
    bucket: str
    per_second_limit: Optional[float]
    concurrent_limit: Optional[float]


class RateLimitExceeded(Exception):
    """
    Exception thrown when the rate limit is exceeded
    """


@contextmanager
def rate_limit(rate_limit_params: RateLimitParameters) -> Iterator[Mapping[str, float]]:
    """
    A context manager for rate limiting that allows for limiting based on
    on a rolling-window per-second rate as well as the number of requests
    concurrently running.

    Uses a single redis sorted set per rate-limiting bucket to track both the
    concurrency and rate, the score is the query timestamp. Queries are thrown
    ahead in time when they start so we can count them as concurrent, and
    thrown back to their start time once they finish so we can count them
    towards the historical rate.

               time >>----->
    +-----------------------------+--------------------------------+
    | historical query window     | currently executing queries    |
    +-----------------------------+--------------------------------+
                                  ^
                                 now
    """

    bucket = '{}{}'.format(state.ratelimit_prefix, rate_limit_params.bucket)
    query_id = uuid.uuid4()

    now = time.time()
    bypass_rate_limit, rate_history_s = state.get_configs([
        ('bypass_rate_limit', 0),
        ('rate_history_sec', 3600)
    ])

    if bypass_rate_limit == 1:
        yield {}

    pipe = state.rds.pipeline(transaction=False)
    pipe.zremrangebyscore(bucket, '-inf', '({:f}'.format(now - rate_history_s))  # cleanup
    pipe.zadd(bucket, now + state.max_query_duration_s, query_id)  # add query
    if rate_limit_params.per_second_limit is None:
        pipe.exists("nosuchkey")  # no-op if we don't need per-second
    else:
        pipe.zcount(bucket, now - state.rate_lookback_s, now)  # get historical
    if rate_limit_params.concurrent_limit is None:
        pipe.exists("nosuchkey")  # no-op if we don't need concurrent
    else:
        pipe.zcount(bucket, '({:f}'.format(now), '+inf')  # get concurrent

    try:
        _, _, historical, concurrent = pipe.execute()
        historical = int(historical)
        concurrent = int(concurrent)
    except Exception as ex:
        logger.exception(ex)
        yield {}  # fail open if redis is having issues

    per_second = historical / float(state.rate_lookback_s)

    rate_limit_name = rate_limit_params.rate_limit_name

    stats: Mapping[str, float] = {
        '{}_rate'.format(rate_limit_name): per_second,
        '{}_concurrent'.format(rate_limit_name): concurrent,

    }

    Reason = namedtuple('reason', 'scope name val limit')
    reasons = [
        Reason(rate_limit_name, 'concurrent', concurrent, rate_limit_params.concurrent_limit),
        Reason(rate_limit_name, 'per-second', per_second, rate_limit_params.per_second_limit),
    ]

    reason = next((r for r in reasons if r.limit is not None and r.val > r.limit), None)

    if reason:
        try:
            state.rds.zrem(bucket, query_id)  # not allowed / not counted
        except Exception as ex:
            logger.exception(ex)
            pass

        raise RateLimitExceeded(
            '{r.scope} {r.name} of {r.val:.0f} exceeds limit of {r.limit:.0f}'.format(r=reason)
        )

    try:
        yield stats
    finally:
        try:
            # return the query to its start time
            state.rds.zincrby(bucket, query_id, -float(state.max_query_duration_s))
        except Exception as ex:
            logger.exception(ex)
            pass


def get_global_rate_limit_params() -> RateLimitParameters:
    """
    Returns the configuration object for the global rate limit
    """

    (per_second, concurr) = state.get_configs([
        ('global_per_second_limit', None),
        ('global_concurrent_limit', 1000),
    ])

    return RateLimitParameters(
        rate_limit_name='global',
        bucket='global',
        per_second_limit=per_second,
        concurrent_limit=concurr,
    )


class RateLimitAggregator(AbstractContextManager):
    """
    Runs the rate limits provided by the `rate_limit_params` configuration object.

    It runs the rate limits in the order they are received and calls the cleanup block
    (__exit__) in the reverse order.
    """

    def __init__(self, rate_limit_params: Sequence[RateLimitParameters]) -> None:
        self.rate_limit_params = rate_limit_params
        self.stack = ExitStack()

    def __enter__(self) -> MutableMapping[str, float]:
        stats: MutableMapping[str, float] = {}

        for rate_limit_param in self.rate_limit_params:
            child_stats: Mapping[str, float] = self.stack.enter_context(rate_limit(rate_limit_param))
            stats.update(child_stats)

        return stats

    def __exit__(
            self,
            exc_type: Optional[Type[BaseException]],
            exc_val: Optional[BaseException],
            exc_tb: Optional[TracebackType]
    ) -> None:
        self.stack.pop_all().close()
