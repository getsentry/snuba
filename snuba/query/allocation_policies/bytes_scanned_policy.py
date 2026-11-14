from __future__ import annotations

import time
from abc import abstractmethod
from collections.abc import Mapping
from enum import StrEnum
from typing import Any

from sentry_redis_tools.sliding_windows_rate_limiter import (
    GrantedQuota,
    Quota,
    RedisSlidingWindowRateLimiter,
    RequestedQuota,
)

from snuba.clickhouse.error_codes import ErrorCodes
from snuba.clickhouse.errors import ClickhouseError
from snuba.configs.configuration import Configuration, ResourceIdentifier
from snuba.query.allocation_policies import (
    CROSS_ORG_SUGGESTION,
    NO_SUGGESTION,
    AllocationPolicy,
    InvalidTenantsForAllocationPolicy,
    QueryResultOrError,
    QuotaAllowance,
)
from snuba.redis import RedisClientKey, get_redis_client

_RATE_LIMITER = RedisSlidingWindowRateLimiter(get_redis_client(RedisClientKey.RATE_LIMITER))

TERABYTE = 10**12
DEFAULT_LIMIT = int(1.28 * TERABYTE)
DEFAULT_MAX_THREADS = 10
DEFAULT_TIMEOUT_PENALTY = 0.025
DEFAULT_TIME_WINDOW = 10 * 60
WINDOW_GRANULARITY_SECONDS = 60


class Enforce(StrEnum):
    ALWAYS = "always"
    PARDON = "pardon"
    NEVER = "never"


class BytesScannedPolicy(AllocationPolicy):
    """Rejects queries once a key has scanned `limit` bytes in a sliding `time_window`.

    Not usable on its own: config names one of the Per*Policy subclasses below,
    each of which picks the key with `_key`, reading tenant ids with `_tenant`.

    Settings come only from the allocation_policies match block, never from
    configurable_component_overrides:

    - limit: bytes allowed per key.
    - enforce: always | pardon | never. pardon lets the query run while cluster
      load is under the block's thresholds.reject; never only records metrics.
      Mapped onto is_enforced / is_pardonable so AllocationPolicy and the
      callers' QuotaAllowance.decision() apply it unchanged.
    - max_threads: threads granted when the query is allowed.
    - time_window: length of the sliding window, in seconds. Defaults to 600
      (10 minutes).
    - timeout_penalty: share of `limit` counted when ClickHouse times out, since a
      timed out query reports no bytes scanned. At 0.025, 40 timeouts fill any key.
    """

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
        super().__init__(storage_key, thresholds=thresholds)
        self._limit = DEFAULT_LIMIT if limit is None else limit
        self._enforce = Enforce(enforce)
        self._max_threads = max_threads
        self._time_window = time_window
        self._timeout_penalty = timeout_penalty

    @abstractmethod
    def _key(self, tenant_ids: dict[str, str | int]) -> str:
        """The counter this query belongs to."""

    def _tenant(self, tenant_ids: dict[str, str | int], field: str) -> str | int:
        """Return tenant_ids[field], rejecting the query if it is missing."""
        value = tenant_ids.get(field)
        if value is None:
            raise InvalidTenantsForAllocationPolicy.from_args(
                tenant_ids, type(self).__name__, f"needs {field}"
            )
        return value

    @property
    def suggestion(self) -> str:
        return (
            "This organization/referrer scanned too many bytes in the last "
            f"{self._time_window} seconds. "
            "This usually means a feature is querying inefficiently or is being abused."
        )

    # Overrides the runtime-config definitions in ConfigurableComponent. Refactor once
    # the old dynamic policy config is removed.
    def _additional_config_definitions(self) -> list[Configuration]:
        return []

    # Overrides the runtime-config lookup in AllocationPolicy. Refactor once the old
    # dynamic policy config is removed.
    @property
    def is_enforced(self) -> bool:
        return self._enforce is not Enforce.NEVER

    # Overrides the runtime-config lookup in AllocationPolicy. Refactor once the old
    # dynamic policy config is removed.
    @property
    def is_pardonable(self) -> bool:
        return self._enforce is Enforce.PARDON

    # Overrides the runtime-config lookup in AllocationPolicy. Refactor once the old
    # dynamic policy config is removed.
    @property
    def max_threads(self) -> int:
        return self._max_threads

    def _quota(self, key: str) -> Quota:
        return Quota(
            window_seconds=self._time_window,
            granularity_seconds=WINDOW_GRANULARITY_SECONDS,
            limit=self._limit,
            # component_name() is unique per storage and class. The window is in the
            # prefix so blocks with different windows on one key never share a counter.
            prefix_override=f"{self.component_name()}-{self._time_window}s-{key}",
        )

    # Abstract hook of the old AllocationPolicy base. Refactor once the old dynamic
    # policy config is removed.
    def _get_quota_allowance(
        self, tenant_ids: dict[str, str | int], query_id: str
    ) -> QuotaAllowance:
        if self.is_cross_org_query(tenant_ids):
            # Cross-org queries have no single org to count against.
            return self._allowance(
                can_run=True,
                used=0,
                explanation={"reason": "cross_org_query"},
                suggestion=CROSS_ORG_SUGGESTION,
            )

        key = self._key(tenant_ids)
        # Requesting the full limit only reads what is left; nothing is used up here.
        # Requesting less would cap the grant and make usage look higher than it is.
        _, granted = _RATE_LIMITER.check_within_quotas(
            [RequestedQuota(self.component_name(), self._limit, [self._quota(key)])]
        )
        used = self._limit - int(granted[0].granted)
        can_run = used < self._limit
        return self._allowance(
            can_run=can_run,
            used=used,
            explanation={
                "reason": "within_limit"
                if can_run
                else f"{key} is at {used} of its {self._limit} bytes limit"
            },
            suggestion=NO_SUGGESTION if can_run else self.suggestion,
        )

    # Abstract hook of the old AllocationPolicy base. Refactor once the old dynamic
    # policy config is removed.
    def _update_quota_balance(
        self,
        tenant_ids: dict[str, str | int],
        query_id: str,
        result_or_error: QueryResultOrError,
    ) -> int:
        if self.is_cross_org_query(tenant_ids):
            return 0
        key = self._key(tenant_ids)
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

    def _bytes_scanned(self, result_or_error: QueryResultOrError) -> int:
        if _is_timeout(result_or_error.error):
            # A timed-out query reports no bytes, so it counts as a share of the limit.
            return int(self._limit * self._timeout_penalty)
        if result_or_error.error or result_or_error.query_result is None:
            return 0
        profile = result_or_error.query_result.result.get("profile") or {}
        return int(profile.get("progress_bytes") or 0)

    def _allowance(
        self, *, can_run: bool, used: int, explanation: dict[str, Any], suggestion: str
    ) -> QuotaAllowance:
        return QuotaAllowance(
            can_run=can_run,
            max_threads=self._max_threads if can_run else 0,
            explanation=explanation,
            is_throttled=False,
            throttle_threshold=self._limit,
            rejection_threshold=self._limit,
            quota_used=used,
            quota_unit="bytes",
            suggestion=suggestion,
        )


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
