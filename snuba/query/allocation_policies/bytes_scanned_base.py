from __future__ import annotations

import time

from sentry_redis_tools.sliding_windows_rate_limiter import (
    GrantedQuota,
    Quota,
    RedisSlidingWindowRateLimiter,
    RequestedQuota,
)

from snuba.query.allocation_policies import AllocationPolicy
from snuba.redis import RedisClientKey, get_redis_client

_RATE_LIMITER = RedisSlidingWindowRateLimiter(get_redis_client(RedisClientKey.RATE_LIMITER))


class BaseBytesScannedPolicy(AllocationPolicy):
    """Sliding-window bookkeeping shared by the bytes-scanned policies. Each subclass
    decides which window a query counts against, its limit and what to do over it."""

    WINDOW_SECONDS = 10 * 60
    WINDOW_GRANULARITY_SECONDS = 60

    def _quota(self, window_prefix: str, limit: int) -> Quota:
        return Quota(
            window_seconds=self.WINDOW_SECONDS,
            granularity_seconds=self.WINDOW_GRANULARITY_SECONDS,
            limit=limit,
            prefix_override=window_prefix,
        )

    def _granted_bytes(self, window_prefix: str, limit: int, requested: int) -> int:
        """How many of `requested` bytes the window still has. Uses up nothing."""
        _, granted_quotas = _RATE_LIMITER.check_within_quotas(
            [RequestedQuota(self.component_name(), requested, [self._quota(window_prefix, limit)])]
        )
        return int(granted_quotas[0].granted)

    def _record_bytes(
        self, request_name: str, window_prefix: str, limit: int, bytes_scanned: int
    ) -> None:
        # The query was already allowed, so record what it used as granted.
        _RATE_LIMITER.use_quotas(
            [RequestedQuota(request_name, bytes_scanned, [self._quota(window_prefix, limit)])],
            grants=[GrantedQuota(request_name, granted=bytes_scanned, reached_quotas=[])],
            timestamp=int(time.time()),
        )
