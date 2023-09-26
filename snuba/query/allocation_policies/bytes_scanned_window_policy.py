from __future__ import annotations

import logging
import time
from typing import Any, cast

from sentry_redis_tools.sliding_windows_rate_limiter import (
    GrantedQuota,
    Quota,
    RedisSlidingWindowRateLimiter,
    RequestedQuota,
)

from snuba.query.allocation_policies import (
    AllocationPolicy,
    AllocationPolicyConfig,
    QueryResultOrError,
    QuotaAllowance,
)
from snuba.redis import RedisClientKey, get_redis_client

logger = logging.getLogger("snuba.query.bytes_scanned_window_policy")


# A hardcoded list of referrers which do not have an organization_id associated with them
# purposefully not in config because we don't want that to be easily changeable
_ORG_LESS_REFERRERS = set(
    [
        "subscriptions_executor",
        "weekly_reports.outcomes",
        "reports.key_errors",
        "reports.key_performance_issues",
        "weekly_reports.key_transactions.this_week",
        "weekly_reports.key_transactions.last_week",
        "dynamic_sampling.distribution.fetch_projects_with_count_per_root_total_volumes",
        "dynamic_sampling.distribution.fetch_orgs_with_count_per_root_total_volumes",
        "dynamic_sampling.counters.fetch_projects_with_count_per_transaction_volumes",
        "dynamic_sampling.counters.fetch_projects_with_transaction_totals",
        "dynamic_sampling.counters.get_org_transaction_volumes",
        "dynamic_sampling.counters.get_active_orgs",
        "migration.backfill_perf_issue_events_issue_platform",
        "api.vroom",
        "replays.query.download_replay_segments",
        "release_monitor.fetch_projects_with_recent_sessions",
        "https://snuba-admin.getsentry.net/",
        "http://localhost:1219/",
        "reprocessing2.start_group_reprocessing",
        "metric_validation",
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
        "_insert_transaction.verify_transaction",
        "tasks.update_user_reports",
        "test.wait_for_event_count",
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
_RATE_LIMITER = RedisSlidingWindowRateLimiter(
    get_redis_client(RedisClientKey.RATE_LIMITER)
)
DEFAULT_OVERRIDE_LIMIT = -1
DEFAULT_BYTES_SCANNED_LIMIT = 10000000


class BytesScannedWindowAllocationPolicy(AllocationPolicy):

    WINDOW_SECONDS = 10 * 60
    WINDOW_GRANULARITY_SECONDS = 60

    def _additional_config_definitions(self) -> list[AllocationPolicyConfig]:
        return [
            AllocationPolicyConfig(
                name="org_limit_bytes_scanned",
                description="Number of bytes any org can scan in a 10 minute window.",
                value_type=int,
                default=DEFAULT_BYTES_SCANNED_LIMIT,
            ),
            AllocationPolicyConfig(
                name="org_limit_bytes_scanned_override",
                description="Number of bytes a specific org can scan in a 10 minute window.",
                value_type=int,
                default=DEFAULT_OVERRIDE_LIMIT,
                param_types={"org_id": int},
            ),
            AllocationPolicyConfig(
                name="throttled_thread_number",
                description="Number of threads any throttled query gets assigned.",
                value_type=int,
                default=1,
            ),
            AllocationPolicyConfig(
                name="use_progress_bytes_scanned",
                description="whether to use the progress.bytes scanned metric to determine the number of bytes scanned in a query, this option should be removed once this metric is used across policies",
                value_type=int,
                default=0,
            ),
        ]

    def _are_tenant_ids_valid(
        self, tenant_ids: dict[str, str | int]
    ) -> tuple[bool, str]:
        if tenant_ids.get("referrer") is None:
            return False, "no referrer"
        if (
            tenant_ids.get("organization_id") is None
            and tenant_ids.get("referrer", None) not in _ORG_LESS_REFERRERS
            and tenant_ids.get("referrer", None) not in _SINGLE_THREAD_REFERRERS
        ):
            return False, f"no organization_id for referrer {tenant_ids['referrer']}"

        return True, ""

    def _get_quota_allowance(
        self, tenant_ids: dict[str, str | int], query_id: str
    ) -> QuotaAllowance:
        ids_are_valid, why = self._are_tenant_ids_valid(tenant_ids)
        if not ids_are_valid:
            if self.is_enforced:
                return QuotaAllowance(
                    can_run=False, max_threads=0, explanation={"reason": why}
                )
        referrer = tenant_ids.get("referrer", "no_referrer")
        org_id = tenant_ids.get("organization_id", None)
        if referrer in _PASS_THROUGH_REFERRERS:
            return QuotaAllowance(True, self.max_threads, {})
        if referrer in _SINGLE_THREAD_REFERRERS:
            return QuotaAllowance(
                can_run=True,
                max_threads=1,
                explanation={"reason": "low priority referrer"},
            )
        if org_id is not None:
            org_limit_bytes_scanned = self.__get_org_limit_bytes_scanned(org_id)

            timestamp, granted_quotas = _RATE_LIMITER.check_within_quotas(
                [
                    RequestedQuota(
                        self.runtime_config_prefix,
                        # request a big number because we don't know how much we actually
                        # will use in this query. this doesn't use up any quota, we just want to know how much is left
                        UNREASONABLY_LARGE_NUMBER_OF_BYTES_SCANNED_PER_QUERY,
                        [
                            Quota(
                                # TODO: Make window configurable but I don't know exactly how the rate limiter
                                # reacts to such changes
                                window_seconds=self.WINDOW_SECONDS,
                                granularity_seconds=self.WINDOW_GRANULARITY_SECONDS,
                                limit=org_limit_bytes_scanned,
                                prefix_override=f"{self.runtime_config_prefix}-organization_id-{org_id}",
                            )
                        ],
                    ),
                ]
            )
            num_threads = self.max_threads
            explanation: dict[str, Any] = {}
            granted_quota = granted_quotas[0]
            if granted_quota.granted <= 0:
                explanation[
                    "reason"
                ] = f"organization {org_id} is over the bytes scanned limit of {org_limit_bytes_scanned}"
                explanation["is_enforced"] = self.is_enforced
                explanation["granted_quota"] = granted_quota.granted
                explanation["limit"] = org_limit_bytes_scanned

                if self.is_enforced:
                    num_threads = self.get_config_value("throttled_thread_number")

            return QuotaAllowance(True, num_threads, explanation)
        return QuotaAllowance(True, self.max_threads, {})

    def _get_bytes_scanned_in_query(
        self, tenant_ids: dict[str, str | int], result_or_error: QueryResultOrError
    ) -> int:
        progress_bytes_scanned = cast(int, result_or_error.query_result.result.get("profile", {}).get("progress_bytes", None))  # type: ignore
        profile_bytes_scanned = cast(int, result_or_error.query_result.result.get("profile", {}).get("bytes", None))  # type: ignore
        if isinstance(progress_bytes_scanned, (int, float)):
            self.metrics.increment(
                "progress_bytes_scanned",
                progress_bytes_scanned,
                tags={"referrer": str(tenant_ids.get("referrer", "no_referrer"))},
            )
        if isinstance(profile_bytes_scanned, (int, float)):
            self.metrics.increment(
                "profile_bytes_scanned",
                profile_bytes_scanned,
                tags={"referrer": str(tenant_ids.get("referrer", "no_referrer"))},
            )
        if self.get_config_value("use_progress_bytes_scanned"):
            return progress_bytes_scanned
        return profile_bytes_scanned

    def _update_quota_balance(
        self,
        tenant_ids: dict[str, str | int],
        query_id: str,
        result_or_error: QueryResultOrError,
    ) -> None:
        if result_or_error.error:
            return
        ids_are_valid, why = self._are_tenant_ids_valid(tenant_ids)
        if not ids_are_valid:
            # we already logged the reason before the query
            return
        bytes_scanned = self._get_bytes_scanned_in_query(tenant_ids, result_or_error)
        query_result = result_or_error.query_result
        assert query_result is not None
        if bytes_scanned is None:
            logging.error("No bytes scanned in query_result %s", query_result)
            return
        if bytes_scanned == 0:
            return
        # we emitted both kinds of bytes scanned in _get_bytes_scanned_in_query however
        # this metric shows what is actually being used to enforce the policy
        self.metrics.increment(
            "bytes_scanned",
            bytes_scanned,
            tags={"referrer": str(tenant_ids.get("referrer", "no_referrer"))},
        )
        if "organization_id" in tenant_ids:
            org_limit_bytes_scanned = self.__get_org_limit_bytes_scanned(
                tenant_ids.get("organization_id")
            )
            # we can assume that the requested quota was granted (because it was)
            # we just need to update the quota with however many bytes were consumed
            _RATE_LIMITER.use_quotas(
                [
                    RequestedQuota(
                        f"{self.runtime_config_prefix}-organization_id-{tenant_ids['organization_id']}",
                        bytes_scanned,
                        [
                            Quota(
                                window_seconds=self.WINDOW_SECONDS,
                                granularity_seconds=self.WINDOW_GRANULARITY_SECONDS,
                                limit=org_limit_bytes_scanned,
                                prefix_override=f"{self.runtime_config_prefix}-organization_id-{tenant_ids['organization_id']}",
                            )
                        ],
                    )
                ],
                grants=[
                    GrantedQuota(
                        f"{self.runtime_config_prefix}-organization_id-{tenant_ids['organization_id']}",
                        granted=bytes_scanned,
                        reached_quotas=[],
                    )
                ],
                timestamp=int(time.time()),
            )

    def __get_org_limit_bytes_scanned(self, org_id: Any) -> int:
        """
        Checks if org specific limit exists and returns that. Returns the "all" orgs
        bytes scanned limit if specific one DNE.
        """
        org_limit_bytes_scanned = self.get_config_value(
            "org_limit_bytes_scanned_override", {"org_id": int(org_id)}
        )
        if org_limit_bytes_scanned == DEFAULT_OVERRIDE_LIMIT:
            org_limit_bytes_scanned = self.get_config_value("org_limit_bytes_scanned")

        return int(org_limit_bytes_scanned)
