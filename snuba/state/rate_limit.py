from abc import ABC, abstractmethod
from collections import namedtuple
from contextlib import AbstractContextManager, ExitStack
from dataclasses import dataclass
import logging
import time
import uuid

from snuba import state, util

logger = logging.getLogger('snuba.state.rate_limit')


@dataclass
class RateLimitParameters:
    bucket: str
    per_second_limit: float
    concurrent_limit: float


class RateLimit(AbstractContextManager, ABC):
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

    def __init__(self, request):
        self._request = request
        self.__did_run = False
        self.__query_id = None
        self.__bucket = None

    @abstractmethod
    def _get_rate_limit_params(self):
        raise NotImplementedError

    @abstractmethod
    def _get_rate_limit_name(self):
        raise NotImplementedError

    def __enter__(self):
        rate_limit_params = self._get_rate_limit_params()

        self.__bucket = '{}{}'.format(state.ratelimit_prefix, rate_limit_params.bucket)
        self.__query_id = uuid.uuid4()
        now = time.time()
        bypass_rate_limit, rate_history_s = state.get_configs([
            ('bypass_rate_limit', 0),
            ('rate_history_sec', 3600)
        ])

        if bypass_rate_limit == 1:
            return (True, 0, 0)

        pipe = state.rds.pipeline(transaction=False)
        pipe.zremrangebyscore(self.__bucket, '-inf', '({:f}'.format(now - rate_history_s))  # cleanup
        pipe.zadd(self.__bucket, now + state.max_query_duration_s, self.__query_id)  # add query
        if rate_limit_params.per_second_limit is None:
            pipe.exists("nosuchkey")  # no-op if we don't need per-second
        else:
            pipe.zcount(self.__bucket, now - state.rate_lookback_s, now)  # get historical
        if rate_limit_params.concurrent_limit is None:
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
            return (True, 0, 0)  # fail open if redis is having issues

        per_second = historical / float(state.rate_lookback_s)

        stats = {
            '{}_rate'.format(self._get_rate_limit_name()): per_second,
            '{}_concurrent'.format(self._get_rate_limit_name()): concurrent,

        }

        Reason = namedtuple('reason', 'scope name val limit')
        reasons = [
            Reason(self._get_rate_limit_name(), 'concurrent', concurrent, rate_limit_params.concurrent_limit),
            Reason(self._get_rate_limit_name(), 'per-second', per_second, rate_limit_params.per_second_limit),
        ]

        reason = next((r for r in reasons if r.limit is not None and r.val > r.limit), None)
        error = reason and '{r.scope} {r.name} of {r.val:.0f} exceeds limit of {r.limit:.0f}'.format(r=reason)

        return error, stats

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            if self.__did_run:
                # return the query to its start time
                state.rds.zincrby(self.__bucket, self.__query_id, -float(state.max_query_duration_s))
            else:
                state.rds.zrem(self.__bucket, self.__query_id)  # not allowed / not counted
        except Exception as ex:
            logger.exception(ex)
            pass


class GlobalRateLimit(RateLimit):
    def _get_rate_limit_name(self):
        return 'global'

    def _get_rate_limit_params(self):
        (per_second, concurr) = state.get_configs([
            ('global_per_second_limit', None),
            ('global_concurrent_limit', 1000),
        ])

        return RateLimitParameters(
            bucket='global',
            per_second_limit=per_second,
            concurrent_limit=concurr,
        )


class ProjectRateLimit(RateLimit):
    def _get_rate_limit_name(self):
        return 'project'

    def _get_rate_limit_params(self):
        assert 'project' in self._request.extensions

        project_ids = util.to_list(self._request.extensions['project']['project'])
        project_id = project_ids[0] if project_ids else 0  # TODO rate limit on every project in the list?

        prl, pcl = state.get_configs([
            ('project_per_second_limit', 1000),
            ('project_concurrent_limit', 1000),
        ])

        # Specific projects can have their rate limits overridden
        (per_second, concurr) = state.get_configs([
            ('project_per_second_limit_{}'.format(project_id), prl),
            ('project_concurrent_limit_{}'.format(project_id), pcl),
        ])

        return RateLimitParameters(
            bucket=str(project_id),
            per_second_limit=per_second,
            concurrent_limit=concurr,
        )


class RateLimitAggregator:
    def __init__(self, rate_limits):
        self.rate_limits = rate_limits
        self.stack = ExitStack()

    def __enter__(self):
        error = None
        stats = {}

        for rate_limit in self.rate_limits:
            if error is None:  # exit early if a rate limit failed
                error, child_stats = self.stack.enter_context(rate_limit)
                stats.update(child_stats)

        return error, stats

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stack.pop_all().close()
