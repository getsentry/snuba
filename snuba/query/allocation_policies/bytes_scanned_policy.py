from __future__ import annotations

import time
from collections.abc import Mapping
from typing import Any, ClassVar, Literal, cast

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
    org_key,
    org_ref_key,
    ref_key,
    setting,
)
from snuba.redis import RedisClientKey, get_redis_client

_RATE_LIMITER = RedisSlidingWindowRateLimiter(get_redis_client(RedisClientKey.RATE_LIMITER))

TERABYTE = 10**12
DEFAULT_TIMEOUT_PENALTY = 0.025
WINDOW_SECONDS = 10 * 60
WINDOW_GRANULARITY_SECONDS = 60


class BytesScannedPolicy(KeyedAllocationPolicy):
    """Rejects queries once a key has scanned `limit` bytes in a sliding 10-minute window.

    Not usable on its own: config names one of the Per*Policy subclasses below,
    each of which picks the key.

    Setting on top of KeyedAllocationPolicy:

    - timeout_penalty: share of `limit` counted when ClickHouse times out, since a
      timed out query reports no bytes scanned. At 0.025, 40 timeouts fill any key.
    """

    quota_unit: ClassVar[Literal["bytes"]] = "bytes"
    DEFAULT_LIMIT: ClassVar[int] = int(1.28 * TERABYTE)
    suggestion: ClassVar[str] = (
        "This organization/referrer scanned too many bytes in the last 10 minutes. "
        "This usually means a feature is querying inefficiently or is being abused."
    )

    def __init__(
        self,
        storage_key: ResourceIdentifier,
        *,
        limit: int | None = None,
        enforce: Enforce | str = Enforce.ALWAYS,
        max_threads: int = DEFAULT_MAX_THREADS,
        thresholds: Mapping[str, Mapping[str, float]] | None = None,
        timeout_penalty: float = DEFAULT_TIMEOUT_PENALTY,
    ) -> None:
        super().__init__(
            storage_key,
            limit=limit,
            enforce=enforce,
            max_threads=max_threads,
            thresholds=thresholds,
        )
        self._timeout_penalty = setting("timeout_penalty", timeout_penalty, float)
        if not 0 <= self._timeout_penalty <= 1:
            raise AllocationPolicyError(
                f"timeout_penalty must be in [0, 1], got {self._timeout_penalty}"
            )

    def _quota(self, key: str) -> Quota:
        return Quota(
            window_seconds=WINDOW_SECONDS,
            granularity_seconds=WINDOW_GRANULARITY_SECONDS,
            limit=self._limit,
            # component_name() is unique per storage and class.
            prefix_override=f"{self.component_name()}-{key}",
        )

    def _check(self, key: str, query_id: str) -> int:
        # Requesting the full limit only reads what is left; nothing is used up here.
        # Requesting less would cap the grant and make usage look higher than it is.
        _, granted = _RATE_LIMITER.check_within_quotas(
            [RequestedQuota(self.component_name(), self._limit, [self._quota(key)])]
        )
        return self._limit - int(granted[0].granted)

    def _bytes_scanned(self, result_or_error: QueryResultOrError) -> int:
        if result_or_error.error:
            cause = result_or_error.error.__cause__
            if isinstance(cause, ClickhouseError) and cause.code == ErrorCodes.TIMEOUT_EXCEEDED:
                return int(self._limit * self._timeout_penalty)
            return 0
        assert result_or_error.query_result is not None
        profile = result_or_error.query_result.result.get("profile") or {}
        progress_bytes = cast(Any, profile).get("progress_bytes")
        return int(progress_bytes) if isinstance(progress_bytes, (int, float)) else 0

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


class BytesScannedPerOrgPolicy(BytesScannedPolicy):
    def _key(self, tenant_ids: dict[str, str | int]) -> str:
        return org_key(tenant_ids, self)


class BytesScannedPerRefPolicy(BytesScannedPolicy):
    def _key(self, tenant_ids: dict[str, str | int]) -> str:
        return ref_key(tenant_ids, self)


class BytesScannedPerOrgRefPolicy(BytesScannedPolicy):
    def _key(self, tenant_ids: dict[str, str | int]) -> str:
        return org_ref_key(tenant_ids, self)
