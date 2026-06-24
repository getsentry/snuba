from __future__ import annotations

import logging

from snuba.configs.configuration import Configuration
from snuba.query.allocation_policies import (
    CROSS_ORG_SUGGESTION,
    MAX_THRESHOLD,
    NO_SUGGESTION,
    PASS_THROUGH_REFERRERS_SUGGESTION,
    InvalidTenantsForAllocationPolicy,
    QueryResultOrError,
    QuotaAllowance,
)
from snuba.query.allocation_policies.concurrent_rate_limit import (
    _PASS_THROUGH_REFERRERS,
    BaseConcurrentRateLimitAllocationPolicy,
)
from snuba.state.rate_limit import RateLimitParameters

logger = logging.getLogger("snuba.query.allocation_policy_dynamic_concurrent_queries")

QUOTA_UNIT = "concurrent_queries"
SUGGESTION = (
    "The cluster is close to its maximum sustainable concurrent query load and this "
    "organization is sending more concurrent queries than its fair share. Reduce the "
    "number of concurrent queries this organization is sending."
)

# The global bucket is shared by every org querying a single storage (and therefore a
# single ClickHouse cluster). Because `component_name()` already prefixes redis keys with
# the storage key, this bucket name only needs to be unique within a storage.
GLOBAL_BUCKET = "__global__"

DEFAULT_GLOBAL_SOFT_LIMIT = 200
DEFAULT_PER_ORG_SOFT_LIMIT = 22


class DynamicConcurrentQueries(BaseConcurrentRateLimitAllocationPolicy):
    """Allows all queries for every org while there is spare capacity on the cluster, and
    sheds load from the heaviest orgs once the cluster approaches its sustainable limit.

    Two soft limits drive the policy:

    * ``global_soft_limit`` - the number of concurrent queries (across every org hitting
      this storage) that we consider "close to the maximum the cluster can sustain". While
      the global concurrent count is at or below this value, every query for every org is
      allowed through.
    * ``per_org_soft_limit`` - the number of concurrent queries each org is softly
      guaranteed. Orgs are only candidates for rejection once they exceed this.

    Once the global concurrent count climbs above ``global_soft_limit`` the policy starts
    shedding load proportionally to how far the cluster is over the soft limit. The
    effective per-org ceiling is::

        effective_limit = max(1, floor(per_org_soft_limit * global_soft_limit / global_concurrent))

    which equals ``per_org_soft_limit`` right at the soft limit and shrinks as global
    pressure rises. Because an org is rejected when its own concurrent count exceeds this
    shrinking ceiling, the orgs that are *most* above their soft limit are rejected first,
    and progressively more orgs get shed as the cluster gets busier. Orgs running a small
    number of queries keep running (an org may always run at least one query), which keeps
    the impact focused on the heaviest abusers.

    Per-org soft limits can be overridden for individual orgs via
    ``organization_soft_limit_override``.
    """

    @property
    def rate_limit_name(self) -> str:
        return "dynamic_concurrent_queries_policy"

    def _additional_config_definitions(self) -> list[Configuration]:
        return super()._additional_config_definitions() + [
            Configuration(
                name="global_soft_limit",
                description=(
                    "Number of concurrent queries (across all orgs on this storage) that "
                    "is considered close to the maximum the cluster can sustain. While the "
                    "global concurrent count is at or below this value, all queries are "
                    "allowed. Above it, the policy starts rejecting queries from the orgs "
                    "that are furthest above their per-org soft limit."
                ),
                value_type=int,
                default=DEFAULT_GLOBAL_SOFT_LIMIT,
            ),
            Configuration(
                name="per_org_soft_limit",
                description=(
                    "Number of concurrent queries each org is softly guaranteed. Orgs at "
                    "or below this are always allowed; orgs above it are the first to be "
                    "rejected once the cluster crosses the global soft limit."
                ),
                value_type=int,
                default=DEFAULT_PER_ORG_SOFT_LIMIT,
            ),
            Configuration(
                name="organization_soft_limit_override",
                description="Override the per-org soft limit for a specific organization_id.",
                value_type=int,
                default=-1,
                param_types={"organization_id": int},
            ),
        ]

    def _get_org_id(self, tenant_ids: dict[str, str | int]) -> int:
        org_id = tenant_ids.get("organization_id")
        if org_id is None:
            raise InvalidTenantsForAllocationPolicy.from_args(
                tenant_ids,
                self.__class__.__name__,
                "tenant_ids must include organization_id",
            )
        return int(org_id)

    def _get_per_org_soft_limit(self, org_id: int) -> int:
        override = self.get_config_value(
            "organization_soft_limit_override", params={"organization_id": org_id}
        )
        if override != -1:
            return int(override)
        return int(self.get_config_value("per_org_soft_limit"))

    def _global_rate_limit_params(self) -> RateLimitParameters:
        return RateLimitParameters(
            self.rate_limit_name,
            bucket=GLOBAL_BUCKET,
            per_second_limit=None,
            concurrent_limit=int(self.get_config_value("global_soft_limit")),
        )

    def _org_rate_limit_params(self, org_id: int, per_org_soft_limit: int) -> RateLimitParameters:
        return RateLimitParameters(
            self.rate_limit_name,
            bucket=str(org_id),
            per_second_limit=None,
            concurrent_limit=per_org_soft_limit,
        )

    def _get_quota_allowance(
        self, tenant_ids: dict[str, str | int], query_id: str
    ) -> QuotaAllowance:
        if tenant_ids.get("referrer", "no_referrer") in _PASS_THROUGH_REFERRERS:
            return QuotaAllowance(
                can_run=True,
                max_threads=self.max_threads,
                explanation={"reason": "pass_through"},
                is_throttled=False,
                throttle_threshold=MAX_THRESHOLD,
                rejection_threshold=MAX_THRESHOLD,
                quota_used=0,
                quota_unit=QUOTA_UNIT,
                suggestion=PASS_THROUGH_REFERRERS_SUGGESTION,
            )
        if self.is_cross_org_query(tenant_ids):
            return QuotaAllowance(
                can_run=True,
                max_threads=self.max_threads,
                explanation={"reason": "cross_org"},
                is_throttled=False,
                throttle_threshold=MAX_THRESHOLD,
                rejection_threshold=MAX_THRESHOLD,
                quota_used=0,
                quota_unit=QUOTA_UNIT,
                suggestion=CROSS_ORG_SUGGESTION,
            )

        org_id = self._get_org_id(tenant_ids)
        global_soft_limit = int(self.get_config_value("global_soft_limit"))
        per_org_soft_limit = self._get_per_org_soft_limit(org_id)

        # Both calls register this query in their respective redis bucket and return the
        # current concurrent count (including this query).
        global_stats, _, _ = self._is_within_rate_limit(query_id, self._global_rate_limit_params())
        org_stats, _, _ = self._is_within_rate_limit(
            query_id, self._org_rate_limit_params(org_id, per_org_soft_limit)
        )

        global_concurrent = global_stats.concurrent
        org_concurrent = org_stats.concurrent

        if global_concurrent == -1 or org_concurrent == -1:
            # the rate limiter errored, fail open
            return QuotaAllowance(
                can_run=True,
                max_threads=self.max_threads,
                explanation={"reason": "rate limiter errored, failing open"},
                is_throttled=False,
                throttle_threshold=MAX_THRESHOLD,
                rejection_threshold=MAX_THRESHOLD,
                quota_used=0,
                quota_unit=QUOTA_UNIT,
                suggestion=NO_SUGGESTION,
            )

        if global_concurrent <= global_soft_limit:
            # The cluster has spare capacity, allow everything.
            can_run = True
            effective_limit = per_org_soft_limit
            why = "within global soft limit"
        else:
            # The cluster is over its global soft limit. Shrink the per-org ceiling
            # proportionally to how far over we are, so the orgs furthest above their
            # soft limit get shed first. An org may always run at least one query.
            effective_limit = max(1, (per_org_soft_limit * global_soft_limit) // global_concurrent)
            can_run = org_concurrent <= effective_limit
            why = (
                f"global concurrent {global_concurrent} exceeds global soft limit "
                f"{global_soft_limit}; org concurrent {org_concurrent} "
                f"{'within' if can_run else 'exceeds'} effective limit {effective_limit}"
            )

        return QuotaAllowance(
            can_run=can_run,
            max_threads=self.max_threads if can_run else 0,
            explanation={
                "reason": why,
                "global_concurrent": global_concurrent,
                "global_soft_limit": global_soft_limit,
                "org_concurrent": org_concurrent,
                "per_org_soft_limit": per_org_soft_limit,
                "effective_org_limit": effective_limit,
            },
            is_throttled=False,
            throttle_threshold=effective_limit,
            rejection_threshold=effective_limit,
            quota_used=org_concurrent,
            quota_unit=QUOTA_UNIT,
            suggestion=NO_SUGGESTION if can_run else SUGGESTION,
        )

    def _update_quota_balance(
        self,
        tenant_ids: dict[str, str | int],
        query_id: str,
        result_or_error: QueryResultOrError,
    ) -> None:
        if self.is_cross_org_query(tenant_ids):
            return
        if tenant_ids.get("referrer", "no_referrer") in _PASS_THROUGH_REFERRERS:
            return
        org_id = self._get_org_id(tenant_ids)
        per_org_soft_limit = self._get_per_org_soft_limit(org_id)
        # Release the query from both buckets it was registered in.
        self._end_query(query_id, self._global_rate_limit_params(), result_or_error)
        self._end_query(
            query_id,
            self._org_rate_limit_params(org_id, per_org_soft_limit),
            result_or_error,
        )
