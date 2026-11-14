from __future__ import annotations

import time
from collections.abc import Mapping
from typing import ClassVar, Literal

from sentry_redis_tools.sliding_windows_rate_limiter import (
    GrantedQuota,
    Quota,
    RedisSlidingWindowRateLimiter,
    RequestedQuota,
)

from snuba.clickhouse.error_codes import ErrorCodes
from snuba.clickhouse.errors import ClickhouseError
from snuba.configs.configuration import ResourceIdentifier
from snuba.query.allocation_policies import AllocationPolicyError, QueryResultOrError
from snuba.query.allocation_policies.keyed_allocation_policy import (
    DEFAULT_MAX_THREADS,
    Enforce,
    KeyedAllocationPolicy,
)
from snuba.redis import RedisClientKey, get_redis_client

_RATE_LIMITER = RedisSlidingWindowRateLimiter(get_redis_client(RedisClientKey.RATE_LIMITER))

TERABYTE = 10**12
DEFAULT_TIMEOUT_PENALTY = 0.025
DEFAULT_TIME_WINDOW = 10 * 60
WINDOW_GRANULARITY_SECONDS = 60


class BytesScannedPolicy(KeyedAllocationPolicy):
    """Rejects queries once a key has scanned `limit` bytes in a sliding `time_window`.

    Not usable on its own: config names one of the Per*Policy subclasses below,
    each of which picks the key.

    Settings on top of KeyedAllocationPolicy:

    - time_window: length of the sliding window, in seconds. Must be a positive
      multiple of 60, the window's granularity. Defaults to 600 (10 minutes).
    - timeout_penalty: share of `limit` counted when ClickHouse times out, since a
      timed out query reports no bytes scanned. At 0.025, 40 timeouts fill any key.
    """

    quota_unit: ClassVar[Literal["bytes"]] = "bytes"
    DEFAULT_LIMIT: ClassVar[int] = int(1.28 * TERABYTE)

    def __init__(
        self,
        storage_key: ResourceIdentifier,
        *,
        limit: int | None = None,
        enforce: Enforce | str = Enforce.ALWAYS,
        max_threads: int = DEFAULT_MAX_THREADS,
        thresholds: Mapping[str, Mapping[str, float]] | None = None,
        time_window: int = DEFAULT_TIME_WINDOW,
        timeout_penalty: float = DEFAULT_TIMEOUT_PENALTY,
    ) -> None:
        super().__init__(
            storage_key,
            limit=limit,
            enforce=enforce,
            max_threads=max_threads,
            thresholds=thresholds,
        )
        self._time_window = time_window
        if self._time_window <= 0 or self._time_window % WINDOW_GRANULARITY_SECONDS:
            raise AllocationPolicyError(
                f"time_window must be a positive multiple of {WINDOW_GRANULARITY_SECONDS} "
                f"seconds, got {self._time_window}"
            )
        self._timeout_penalty = timeout_penalty
        if not 0 <= self._timeout_penalty <= 1:
            raise AllocationPolicyError(
                f"timeout_penalty must be in [0, 1], got {self._timeout_penalty}"
            )

    @property
    def suggestion(self) -> str:
        return (
            "This organization/referrer scanned too many bytes in the last "
            f"{self._time_window} seconds. "
            "This usually means a feature is querying inefficiently or is being abused."
        )

    def _quota(self, key: str) -> Quota:
        return Quota(
            window_seconds=self._time_window,
            granularity_seconds=WINDOW_GRANULARITY_SECONDS,
            limit=self._limit,
            # component_name() is unique per storage and class. The window is in the
            # prefix so blocks with different windows on one key never share a counter.
            prefix_override=f"{self.component_name()}-{self._time_window}s-{key}",
        )

    def _check(self, key: str, query_id: str) -> int:
        # Requesting the full limit only reads what is left; nothing is used up here.
        # Requesting less would cap the grant and make usage look higher than it is.
        _, granted = _RATE_LIMITER.check_within_quotas(
            [RequestedQuota(self.component_name(), self._limit, [self._quota(key)])]
        )
        return self._limit - int(granted[0].granted)

    def _bytes_scanned(self, result_or_error: QueryResultOrError) -> int:
        if _is_timeout(result_or_error.error):
            # A timed-out query reports no bytes, so it counts as a share of the limit.
            return int(self._limit * self._timeout_penalty)
        if result_or_error.error or result_or_error.query_result is None:
            return 0
        profile = result_or_error.query_result.result.get("profile") or {}
        return int(profile.get("progress_bytes") or 0)

    def _record(self, key: str, query_id: str, result_or_error: QueryResultOrError) -> int:
        bytes_scanned = self._bytes_scanned(result_or_error)
        if bytes_scanned <= 0:
            return 0
        request_name = self.component_name()
        # The query already ran, so record what it scanned as granted.
        _RATE_LIMITER.use_quotas(
            [RequestedQuota(request_name, bytes_scanned, [self._quota(key)])],
            grants=[GrantedQuota(request_name, granted=bytes_scanned, reached_quotas=[])],
            timestamp=int(time.time()),
        )
        return bytes_scanned


def _is_timeout(error: Exception | None) -> bool:
    cause = error.__cause__ if error else None
    return isinstance(cause, ClickhouseError) and cause.code == ErrorCodes.TIMEOUT_EXCEEDED


class BytesScannedPerOrgPolicy(BytesScannedPolicy):
    """One counter per organization, across all its referrers."""

    def _key(self, tenant_ids: dict[str, str | int]) -> str:
        return f"org:{self._tenant(tenant_ids, 'organization_id')}"


class BytesScannedPerRefPolicy(BytesScannedPolicy):
    """One counter per referrer, shared by all organizations."""

    def _key(self, tenant_ids: dict[str, str | int]) -> str:
        return f"ref:{self._tenant(tenant_ids, 'referrer')}"


class BytesScannedPerOrgRefPolicy(BytesScannedPolicy):
    """One counter per organization and referrer."""

    def _key(self, tenant_ids: dict[str, str | int]) -> str:
        org = self._tenant(tenant_ids, "organization_id")
        ref = self._tenant(tenant_ids, "referrer")
        return f"org:{org}|ref:{ref}"
