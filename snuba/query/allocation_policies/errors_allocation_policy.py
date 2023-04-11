from __future__ import annotations

import logging
import time

from snuba import environment
from snuba.query.allocation_policies import (
    DEFAULT_PASSTHROUGH_POLICY,
    AllocationPolicy,
    QueryResultOrError,
    QuotaAllowance,
)
from snuba.state import get_config
from snuba.state.sliding_windows import (
    GrantedQuota,
    Quota,
    RedisSlidingWindowRateLimiter,
    RequestedQuota,
)
from snuba.utils.metrics.wrapper import MetricsWrapper

metrics = MetricsWrapper(environment.metrics, "errors_allocation_policy")


# A hardcoded list of referrers which do not have an organization_id associated with them
# purposefully not in config because we don't want that to be easily changeable
_ORG_LESS_REFERRERS = set(
    [
        "weekly_reports.outcomes",
        "reports.key_errors",
        "weekly_reports.key_transactions.this_week",
        "weekly_reports.key_transactions.last_week",
        "dynamic_sampling.distribution.fetch_projects_with_count_per_root_total_volumes",
        "reports.key_performance_issues",
        "dynamic_sampling.counters.fetch_projects_with_count_per_transaction_volumes",
        "dynamic_sampling.counters.fetch_projects_with_transaction_totals",
        "migration.backfill_perf_issue_events_issue_platform",
        "api.vroom",
        "replays.query.download_replay_segments",
        "outcomes.timeseries",
        "release_monitor.fetch_projects_with_recent_sessions",
        "https://snuba-admin.getsentry.net/",
        "reprocessing2.start_group_reprocessing",
    ]
)


class ErrorsAllocationPolicy(AllocationPolicy):

    WINDOW_SECONDS = 10 * 60
    WINDOW_GRANULARITY_SECONDS = 60

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._rate_limiter = RedisSlidingWindowRateLimiter()

    @property
    def rate_limit_prefix(self) -> str:
        return self.__class__.__name__

    def _are_tenant_ids_valid(
        self, tenant_ids: dict[str, str | int]
    ) -> tuple[bool, str]:
        if "referrer" not in tenant_ids:
            return False, "no referrer"
        if (
            "organization_id" not in tenant_ids
            and tenant_ids.get("referrer", None) not in _ORG_LESS_REFERRERS
        ):
            return False, "no organization_id for referrer {tenant_ids['referrer']}"
        return True, ""

    def _get_quota_allowance(self, tenant_ids: dict[str, str | int]):
        # TODO: This kind of killswitch should just be included with every allocation policy
        is_active = get_config(f"{self.rate_limit_prefix}.is_active", True)
        is_enforced = get_config(f"{self.rate_limit_prefix}.is_enforced", False)
        if not is_active:
            return DEFAULT_PASSTHROUGH_POLICY.get_quota_allowance(tenant_ids)
        ids_are_valid, why = self._are_tenant_ids_valid(tenant_ids)
        if not ids_are_valid and is_enforced:
            metrics.increment(
                "db_request_rejected",
                tags={
                    "storage_set_key": self._storage_set_key.value,
                    "is_enforced": str(is_enforced),
                },
            )
            return QuotaAllowance(
                can_run=False, max_threads=0, explanation={"reason": why}
            )
        if "organization_id" in tenant_ids:
            org_scan_limit = get_config(
                # TODO: figure out an actually good number
                f"{self.rate_limit_prefix}.org_scan_limit",
                10000,
            )

            timestamp, granted_quotas = self._rate_limiter.check_within_quotas(
                [
                    RequestedQuota(
                        self.rate_limit_prefix,
                        # request a big number because we don't know how much we actually
                        # will use in this query. this doesn't use up any quota, we just want to know how much is left
                        int(1e10),
                        [
                            Quota(
                                # TODO: Make window configurable but I don't know exactly how the rate limiter
                                # reacts to such changes
                                window_seconds=self.WINDOW_SECONDS,
                                granularity_seconds=self.WINDOW_GRANULARITY_SECONDS,
                                limit=int(org_scan_limit),  # type: ignore
                                prefix_override=f"{self.rate_limit_prefix}-organization_id-{tenant_ids['organization_id']}",
                            )
                        ],
                    ),
                ]
            )
            num_threads = 8
            explanation = {}
            granted_quota = granted_quotas[0]
            if granted_quota.granted <= 0:
                metrics.increment(
                    "db_request_throttled",
                    tags={
                        "storage_set_key": self._storage_set_key.value,
                        "is_enforced": str(is_enforced),
                    },
                )
                if is_enforced:
                    num_threads = 1
                    explanation[
                        "reason"
                    ] = f"organization {tenant_ids['organization_id']} is over the bytes scanned limit of {org_scan_limit}"

            return QuotaAllowance(True, num_threads, explanation)
        return QuotaAllowance(True, 8, {})

    def _update_quota_balance(
        self,
        tenant_ids: dict[str, str | int],
        result_or_error: QueryResultOrError,
    ) -> None:
        is_active = get_config(f"{self.rate_limit_prefix}.is_active", True)
        if not is_active:
            return
        if result_or_error.error:
            return
        query_result = result_or_error.query_result
        assert query_result is not None
        bytes_scanned = query_result.result["profile"]["bytes"]  # type: ignore
        if not bytes_scanned:
            logging.error("No bytes scanned in query_result")
            return
        org_scan_limit = get_config(f"{self.rate_limit_prefix}.org_scan_limit", 10000)
        # we can assume that the requested quota was granted (because it was)
        # we just need to update the quota with however many bytes were consumed
        self._rate_limiter.use_quotas(
            [
                RequestedQuota(
                    "{self.rate_limit_prefix}-organization_id-{tenant_ids['organization_id']}",
                    bytes_scanned,
                    [
                        Quota(
                            window_seconds=self.WINDOW_SECONDS,
                            granularity_seconds=self.WINDOW_GRANULARITY_SECONDS,
                            limit=int(org_scan_limit),  # type: ignore
                            prefix_override=f"{self.rate_limit_prefix}-organization_id-{tenant_ids['organization_id']}",
                        )
                    ],
                )
            ],
            grants=[
                GrantedQuota(
                    "{self.rate_limit_prefix}-organization_id-{tenant_ids['organization_id']}",
                    granted=bytes_scanned,
                    reached_quotas=[],
                )
            ],
            timestamp=int(time.time()),
        )
