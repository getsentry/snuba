from collections import namedtuple
from contextlib import AbstractContextManager, ExitStack
from dataclasses import dataclass
import logging
import time
from types import TracebackType
from typing import Mapping, MutableMapping, Optional, Sequence, Type
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


class RateLimit(AbstractContextManager):
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

    def __init__(self, rate_limit_params: RateLimitParameters) -> None:
        self.__rate_limit_params = rate_limit_params
        self.__did_run = False
        self.__query_id = uuid.uuid4()
        self.__bucket = '{}{}'.format(state.ratelimit_prefix, self.__rate_limit_params.bucket)

    def __enter__(self) -> Mapping[str, float]:
        now = time.time()
        bypass_rate_limit, rate_history_s = state.get_configs([
            ('bypass_rate_limit', 0),
            ('rate_history_sec', 3600)
        ])

        if bypass_rate_limit == 1:
            return {}

        pipe = state.rds.pipeline(transaction=False)
        pipe.zremrangebyscore(self.__bucket, '-inf', '({:f}'.format(now - rate_history_s))  # cleanup
        pipe.zadd(self.__bucket, now + state.max_query_duration_s, self.__query_id)  # add query
        if self.__rate_limit_params.per_second_limit is None:
            pipe.exists("nosuchkey")  # no-op if we don't need per-second
        else:
            pipe.zcount(self.__bucket, now - state.rate_lookback_s, now)  # get historical
        if self.__rate_limit_params.concurrent_limit is None:
            pipe.exists("nosuchkey")  # no-op if we don't need concurrent
        else:
            pipe.zcount(self.__bucket, '({:f}'.format(now), '+inf')  # get concurrent

        try:
            _, _, historical, concurrent = pipe.execute()
            historical = int(historical)
            concurrent = int(concurrent)
            self.__did_run = True
        except Exception as ex:
            logger.exception(ex)
            return {}  # fail open if redis is having issues

        per_second = historical / float(state.rate_lookback_s)

        rate_limit_name = self.__rate_limit_params.rate_limit_name

        stats: Mapping[str, float] = {
            '{}_rate'.format(rate_limit_name): per_second,
            '{}_concurrent'.format(rate_limit_name): concurrent,

        }

        Reason = namedtuple('reason', 'scope name val limit')
        reasons = [
            Reason(rate_limit_name, 'concurrent', concurrent, self.__rate_limit_params.concurrent_limit),
            Reason(rate_limit_name, 'per-second', per_second, self.__rate_limit_params.per_second_limit),
        ]

        reason = next((r for r in reasons if r.limit is not None and r.val > r.limit), None)

        if reason:
            self.__return_query_to_start_time()
            raise RateLimitExceeded(
                '{r.scope} {r.name} of {r.val:.0f} exceeds limit of {r.limit:.0f}'.format(r=reason)
            )

        return stats

    def __return_query_to_start_time(self) -> None:
        state.rds.zincrby(self.__bucket, self.__query_id, -float(state.max_query_duration_s))

    def __exit__(
            self,
            exc_type: Optional[Type[BaseException]],
            exc_val: Optional[BaseException],
            exc_tb: Optional[TracebackType]
    ) -> None:
        try:
            if self.__did_run:
                self.__return_query_to_start_time()
            else:
                state.rds.zrem(self.__bucket, self.__query_id)  # not allowed / not counted
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
        self.rate_limits = map(RateLimit, rate_limit_params)
        self.stack = ExitStack()

    def __enter__(self) -> MutableMapping[str, float]:
        stats: MutableMapping[str, float] = {}

        for rate_limit in self.rate_limits:
            child_stats = self.stack.enter_context(rate_limit)
            stats.update(child_stats)

        return stats

    def __exit__(
            self,
            exc_type: Optional[Type[BaseException]],
            exc_val: Optional[BaseException],
            exc_tb: Optional[TracebackType]
    ) -> None:
        self.stack.pop_all().close()
