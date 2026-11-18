from __future__ import annotations

from abc import abstractmethod
from collections.abc import Mapping
from typing import Any

from snuba.configs.configuration import Configuration, ResourceIdentifier
from snuba.query.allocation_policies import (
    CROSS_ORG_SUGGESTION,
    MAX_THRESHOLD,
    NO_SUGGESTION,
    AllocationPolicy,
    AllocationPolicyViolations,
    InvalidTenantsForAllocationPolicy,
    QueryResultOrError,
    QuotaAllowance,
)
from snuba.query.allocation_policies.bytes_scanned_policy import Enforce
from snuba.state.rate_limit import (
    RateLimitParameters,
    rate_limit_finish_request,
    rate_limit_start_request,
)

DEFAULT_LIMIT = 22
DEFAULT_MAX_THREADS = 10
# Only the concurrent count is used, so finished queries need not be kept.
RATE_HISTORY_S = 1
SHARD_FACTOR = 1
QUOTA_UNIT = "concurrent_queries"


class ConcurrentQueriesPolicy(AllocationPolicy):
    """Rejects a query when above `limit` queries running per `key`.

    Config names a Per*Policy subclass, which picks the key. Settings come only from
    the allocation_policies match block, not configurable_component_overrides:

    - limit: concurrent queries allowed per key.
    - enforce: always | pardon | never.
    - max_threads: threads granted when the query is allowed.
    """

    # Cross-org queries always run and are not counted, unless this is True.
    _counts_cross_org = False

    def __init__(
        self,
        storage_key: ResourceIdentifier,
        *,
        limit: int = DEFAULT_LIMIT,
        enforce: Enforce | str = Enforce.ALWAYS,
        max_threads: int = DEFAULT_MAX_THREADS,
        thresholds: Mapping[str, Mapping[str, float]] | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(storage_key, thresholds=thresholds, **kwargs)
        self._limit = limit
        self._enforce = Enforce(enforce)
        self._max_threads = max_threads

    @abstractmethod
    def _key(self, tenant_ids: dict[str, str | int]) -> str:
        raise NotImplementedError

    def _tenant(self, tenant_ids: dict[str, str | int], field: str) -> str | int:
        value = tenant_ids.get(field)
        if value is None:
            raise InvalidTenantsForAllocationPolicy.from_args(
                tenant_ids, type(self).__name__, f"needs {field}"
            )
        return value

    @property
    def suggestion(self) -> str:
        return (
            "This organization/referrer has too many queries running at once. "
            "This usually means a feature is querying inefficiently or is being abused."
        )

    # The next four override runtime-config lookups. Refactor once the old dynamic
    # policy config is removed.
    def _additional_config_definitions(self) -> list[Configuration]:
        return []

    @property
    def is_enforced(self) -> bool:
        return self._enforce is not Enforce.NEVER

    @property
    def is_pardonable(self) -> bool:
        return self._enforce is Enforce.PARDON

    @property
    def max_threads(self) -> int:
        return self._max_threads

    # Abstract hook of the old AllocationPolicy base. Refactor once the old dynamic
    # policy config is removed.
    def _get_quota_allowance(
        self, tenant_ids: dict[str, str | int], query_id: str
    ) -> QuotaAllowance:
        if not self._counts_cross_org and self.is_cross_org_query(tenant_ids):
            # Cross-org queries have no single org to count against.
            return self._allowance(
                can_run=True,
                used=0,
                reason="cross_org_query",
                suggestion=CROSS_ORG_SUGGESTION,
                threshold=MAX_THRESHOLD,
            )

        key = self._key(tenant_ids)
        # Registers the query as running. The count includes it, and is -1 (runs)
        # when Redis fails.
        running = rate_limit_start_request(
            RateLimitParameters(
                self.component_name(),
                bucket=key,
                per_second_limit=None,
                concurrent_limit=self._limit,
            ),
            query_id,
            RATE_HISTORY_S,
            SHARD_FACTOR,
            f"{self.component_name()}:",
        ).concurrent
        if running <= self._limit:
            return self._allowance(
                can_run=True,
                used=running,
                reason="within_limit",
                suggestion=NO_SUGGESTION,
                threshold=self._limit,
            )
        return self._allowance(
            can_run=False,
            used=running,
            reason=f"{key} has {running} of its {self._limit} concurrent queries running",
            suggestion=self.suggestion,
            threshold=self._limit,
        )

    # Abstract hook of the old AllocationPolicy base. Refactor once the old dynamic
    # policy config is removed.
    def _update_quota_balance(
        self,
        tenant_ids: dict[str, str | int],
        query_id: str,
        result_or_error: QueryResultOrError,
    ) -> None:
        # Skips exactly the queries _get_quota_allowance did not register.
        if not self._counts_cross_org and self.is_cross_org_query(tenant_ids):
            return
        # Removes the query from the running set. A rejected query is dropped
        # entirely; a finished one is kept at its start time.
        was_rejected = result_or_error.error is not None and isinstance(
            result_or_error.error.__cause__, AllocationPolicyViolations
        )
        rate_limit_finish_request(
            RateLimitParameters(
                self.component_name(),
                bucket=self._key(tenant_ids),
                per_second_limit=None,
                concurrent_limit=self._limit,
            ),
            query_id,
            SHARD_FACTOR,
            was_rejected,
            f"{self.component_name()}:",
        )

    def _allowance(
        self, *, can_run: bool, used: int, reason: str, suggestion: str, threshold: int
    ) -> QuotaAllowance:
        return QuotaAllowance(
            can_run=can_run,
            max_threads=self._max_threads if can_run else 0,
            explanation={"reason": reason},
            is_throttled=False,
            throttle_threshold=threshold,
            rejection_threshold=threshold,
            quota_used=used,
            quota_unit=QUOTA_UNIT,
            suggestion=suggestion,
        )


class ConcurrentQueriesPerOrgPolicy(ConcurrentQueriesPolicy):
    """One count per organization, across all its referrers."""

    def _key(self, tenant_ids: dict[str, str | int]) -> str:
        return f"org:{self._tenant(tenant_ids, 'organization_id')}"


class ConcurrentQueriesPerRefPolicy(ConcurrentQueriesPolicy):
    """One count per referrer, shared by all organizations.

    Replaces ReferrerGuardRailPolicy: counts cross-org queries too, and puts
    queries without a referrer under "no_referrer".
    """

    _counts_cross_org = True

    def __init__(self, storage_key: ResourceIdentifier, *, limit: int = 100, **kwargs: Any) -> None:
        super().__init__(storage_key, limit=limit, **kwargs)

    def _key(self, tenant_ids: dict[str, str | int]) -> str:
        return f"ref:{tenant_ids.get('referrer', 'no_referrer')}"


class ConcurrentQueriesPerOrgRefPolicy(ConcurrentQueriesPolicy):
    """One count per organization and referrer."""

    def _key(self, tenant_ids: dict[str, str | int]) -> str:
        org = self._tenant(tenant_ids, "organization_id")
        ref = self._tenant(tenant_ids, "referrer")
        return f"org:{org}|ref:{ref}"
