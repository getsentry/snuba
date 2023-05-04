from __future__ import annotations

import logging
import time
from typing import Any, cast

from snuba import environment
from snuba.clusters.storage_sets import StorageSetKey
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

# A hardcoded list of referrers which do not have an organization_id associated with them
# purposefully not in config because we don't want that to be easily changeable
_ORG_LESS_REFERRERS = set(
    [
        "subscriptions_executor",
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
        "release_monitor.fetch_projects_with_recent_sessions",
        "https://snuba-admin.getsentry.net/",
        "reprocessing2.start_group_reprocessing",
        # This is just a suite of sentry tests that I do not want to
        # update
        "_insert_transaction.verify_transaction",
    ]
)


# referrers which do not serve the UI and are given low capacity by default
_SINGLE_THREAD_REFERRERS = set(
    [
        "delete-events-from-file",
        "delete-event-user-data",
        "scrub-nodestore",
        "fetch_events_for_deletion",
        "delete-events-by-tag-value",
        "delete.fetch_last_group",
        "forward-events",
        "tasks.update_user_reports",
    ]
)


# subscriptions currently do not undergo rate limiting in any way.
# having subscriptions be too slow means there is an incident
_PASS_THROUGH_REFERRERS = set(
    [
        "subscriptions_executor",
    ]
)


UNREASONABLY_LARGE_NUMBER_OF_BYTES_SCANNED_PER_QUERY = int(1e10)
_RATE_LIMITER = RedisSlidingWindowRateLimiter()


class BytesScannedWindowAllocationPolicy(AllocationPolicy):

    WINDOW_SECONDS = 10 * 60
    WINDOW_GRANULARITY_SECONDS = 60

    def __init__(
        self,
        storage_set_key: StorageSetKey,
        required_tenant_types: list[str],
        **kwargs: str,
    ) -> None:
        super().__init__(storage_set_key, required_tenant_types)

    @property
    def metrics(self) -> MetricsWrapper:
        return MetricsWrapper(environment.metrics, self.__class__.__name__)

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
            and tenant_ids.get("referrer", None) not in _SINGLE_THREAD_REFERRERS
        ):
            return False, f"no organization_id for referrer {tenant_ids['referrer']}"
        return True, ""

    def _get_quota_allowance(self, tenant_ids: dict[str, str | int]) -> QuotaAllowance:
        # TODO: This kind of killswitch should just be included with every allocation policy
        is_active = cast(bool, get_config(f"{self.rate_limit_prefix}.is_active", True))
        is_enforced = cast(
            bool, get_config(f"{self.rate_limit_prefix}.is_enforced", True)
        )
        throttled_thread_number = cast(
            int, get_config(f"{self.rate_limit_prefix}.throttled_thread_number", 1)
        )
        max_threads = cast(int, get_config("query_settings/max_threads", 8))
        if not is_active:
            return DEFAULT_PASSTHROUGH_POLICY.get_quota_allowance(tenant_ids)
        ids_are_valid, why = self._are_tenant_ids_valid(tenant_ids)
        if not ids_are_valid:
            self.metrics.increment(
                "db_request_rejected",
                tags={
                    "storage_set_key": self._storage_set_key.value,
                    "is_enforced": str(is_enforced),
                    "referrer": str(tenant_ids.get("referrer", "no_referrer")),
                },
            )
            if is_enforced:
                return QuotaAllowance(
                    can_run=False, max_threads=0, explanation={"reason": why}
                )
        referrer = tenant_ids.get("referrer", "no_referrer")
        org_id = tenant_ids.get("organization_id", None)
        if referrer in _PASS_THROUGH_REFERRERS:
            return DEFAULT_PASSTHROUGH_POLICY.get_quota_allowance(tenant_ids)
        if referrer in _SINGLE_THREAD_REFERRERS:
            return QuotaAllowance(
                can_run=True,
                max_threads=1,
                explanation={"reason": "low priority referrer"},
            )
        if org_id is not None:
            org_limit_bytes_scanned = cast(
                int,
                get_config(
                    # TODO: figure out an actually good number
                    f"{self.rate_limit_prefix}.org_limit_bytes_scanned",
                    10000,
                ),
            )

            timestamp, granted_quotas = _RATE_LIMITER.check_within_quotas(
                [
                    RequestedQuota(
                        self.rate_limit_prefix,
                        # request a big number because we don't know how much we actually
                        # will use in this query. this doesn't use up any quota, we just want to know how much is left
                        UNREASONABLY_LARGE_NUMBER_OF_BYTES_SCANNED_PER_QUERY,
                        [
                            Quota(
                                # TODO: Make window configurable but I don't know exactly how the rate limiter
                                # reacts to such changes
                                window_seconds=self.WINDOW_SECONDS,
                                granularity_seconds=self.WINDOW_GRANULARITY_SECONDS,
                                limit=int(org_limit_bytes_scanned),
                                prefix_override=f"{self.rate_limit_prefix}-organization_id-{org_id}",
                            )
                        ],
                    ),
                ]
            )
            num_threads = max_threads
            explanation: dict[str, Any] = {}
            granted_quota = granted_quotas[0]
            if granted_quota.granted <= 0:
                self.metrics.increment(
                    "db_request_throttled",
                    tags={
                        "storage_set_key": self._storage_set_key.value,
                        "is_enforced": str(is_enforced),
                        "referrer": str(tenant_ids.get("referrer", "no_referrer")),
                    },
                )
                explanation[
                    "reason"
                ] = f"organization {org_id} is over the bytes scanned limit of {org_limit_bytes_scanned}"
                explanation["is_enforced"] = is_enforced
                explanation["granted_quota"] = granted_quota.granted
                explanation["limit"] = org_limit_bytes_scanned

                if is_enforced:
                    num_threads = throttled_thread_number

            return QuotaAllowance(True, num_threads, explanation)
        return QuotaAllowance(True, max_threads, {})

    def _update_quota_balance(
        self,
        tenant_ids: dict[str, str | int],
        result_or_error: QueryResultOrError,
    ) -> None:
        if not get_config(f"{self.rate_limit_prefix}.is_active", True):
            return
        if result_or_error.error:
            return
        ids_are_valid, why = self._are_tenant_ids_valid(tenant_ids)
        if not ids_are_valid:
            # we already logged the reason before the query
            return
        query_result = result_or_error.query_result
        assert query_result is not None
        bytes_scanned = query_result.result.get("profile", {}).get("bytes", None)  # type: ignore
        if bytes_scanned is None:
            logging.error("No bytes scanned in query_result %s", query_result)
            return
        if bytes_scanned == 0:
            return
        if "organization_id" in tenant_ids:
            org_limit_bytes_scanned = get_config(
                f"{self.rate_limit_prefix}.org_limit_bytes_scanned", 10000
            )
            # we can assume that the requested quota was granted (because it was)
            # we just need to update the quota with however many bytes were consumed
            _RATE_LIMITER.use_quotas(
                [
                    RequestedQuota(
                        f"{self.rate_limit_prefix}-organization_id-{tenant_ids['organization_id']}",
                        bytes_scanned,
                        [
                            Quota(
                                window_seconds=self.WINDOW_SECONDS,
                                granularity_seconds=self.WINDOW_GRANULARITY_SECONDS,
                                limit=int(org_limit_bytes_scanned),  # type: ignore
                                prefix_override=f"{self.rate_limit_prefix}-organization_id-{tenant_ids['organization_id']}",
                            )
                        ],
                    )
                ],
                grants=[
                    GrantedQuota(
                        f"{self.rate_limit_prefix}-organization_id-{tenant_ids['organization_id']}",
                        granted=bytes_scanned,
                        reached_quotas=[],
                    )
                ],
                timestamp=int(time.time()),
            )
