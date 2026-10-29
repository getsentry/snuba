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

from snuba.clickhouse.error_codes import ErrorCodes
from snuba.clickhouse.errors import ClickhouseError
from snuba.configs.configuration import Configuration
from snuba.query.allocation_policies import (
    CROSS_ORG_SUGGESTION,
    MAX_THRESHOLD,
    NO_SUGGESTION,
    PASS_THROUGH_REFERRERS_SUGGESTION,
    AllocationPolicy,
    InvalidTenantsForAllocationPolicy,
    QueryResultOrError,
    QuotaAllowance,
)
from snuba.redis import RedisClientKey, get_redis_client

logger = logging.getLogger("snuba.query.bytes_scanned_window_policy")


# we don't limit the amount of bytes subscriptions can scan at this time
_PASS_THROUGH_REFERRERS = {
    "subscriptions_executor",
}


UNREASONABLY_LARGE_NUMBER_OF_BYTES_SCANNED_PER_QUERY = int(1e12)
_RATE_LIMITER = RedisSlidingWindowRateLimiter(get_redis_client(RedisClientKey.RATE_LIMITER))
DEFAULT_OVERRIDE_LIMIT = -1
TERABYTE = 10**12
DEFAULT_BYTES_SCANNED_LIMIT = int(1.28 * TERABYTE)
DEFAULT_TIMEOUT_PENALIZATION = DEFAULT_BYTES_SCANNED_LIMIT // 40
DEFAULT_BYTES_THROTTLE_DIVIDER = 1.5
DEFAULT_THREADS_THROTTLE_DIVIDER = 2
QUOTA_UNIT = "bytes"
KEY_BY_TENANT_REFERRER = "tenant_referrer"
KEY_BY_ORGANIZATION_REFERRER = "organization_referrer"
KEY_BY_ORGANIZATION = "organization"
_KEY_BY_VALUES = {KEY_BY_TENANT_REFERRER, KEY_BY_ORGANIZATION_REFERRER, KEY_BY_ORGANIZATION}
SUGGESTION = "The feature, organization/project is scanning too many bytes, this usually means they are abusing that API"


class BytesScannedRejectingPolicy(AllocationPolicy):
    """For every query that comes in, keep track of the amount of bytes scanned for every
        (project_id|organization_id, referrer)
    combination in the last 10 minutes (sliding window). If a specific combination scans too
    many bytes, reject that query

    cross-project queries use the organization_id, single project queries use the project_id.
    `key_by` can count the window per (organization_id, referrer) or per organization_id instead.
    """

    WINDOW_SECONDS = 10 * 60
    WINDOW_GRANULARITY_SECONDS = 60

    def _additional_config_definitions(self) -> list[Configuration]:
        return [
            Configuration(
                "scan_limit",
                f"How many bytes a tenant (see key_by) can scan in the last {self.WINDOW_SECONDS / 60} mins before queries start getting rejected. Per-tenant values go in match blocks. Unset (-1) falls back to project_referrer_scan_limit or organization_referrer_scan_limit",
                int,
                DEFAULT_OVERRIDE_LIMIT,
            ),
            Configuration(
                "key_by",
                f"Which tenant the scan window counts against: {KEY_BY_TENANT_REFERRER} (project_id if present, else organization_id, plus referrer), {KEY_BY_ORGANIZATION_REFERRER} or {KEY_BY_ORGANIZATION}. Queries without an organization_id use {KEY_BY_TENANT_REFERRER}",
                str,
                KEY_BY_TENANT_REFERRER,
            ),
            Configuration(
                "max_bytes_to_read",
                "Hard cap forwarded to clickhouse as max_bytes_to_read for org-keyed queries (no project_id). Queries it applies to run with this cap and bypass the sliding-window scan limit. Set it per org or referrer in a match block. Unset is -1",
                int,
                DEFAULT_OVERRIDE_LIMIT,
            ),
            Configuration(
                "project_referrer_scan_limit",
                f"Used when scan_limit is unset: how many bytes a project can scan per referrer in the last {self.WINDOW_SECONDS / 60} mins before queries start getting rejected",
                int,
                DEFAULT_BYTES_SCANNED_LIMIT,
            ),
            Configuration(
                "organization_referrer_scan_limit",
                f"Used when scan_limit is unset: how many bytes an organization can scan per referrer in the last {self.WINDOW_SECONDS / 60} mins before queries start getting rejected. Cross-project queries are limited by organization_id",
                int,
                DEFAULT_BYTES_SCANNED_LIMIT * 2,
            ),
            Configuration(
                "clickhouse_timeout_bytes_scanned_penalization",
                "If a clickhouse query times out, how many bytes does the policy assume the query scanned? Increasing the number increases the penalty for queries that time out",
                int,
                DEFAULT_TIMEOUT_PENALIZATION,
            ),
            Configuration(
                "bytes_throttle_divider",
                "Divide the scan limit by this number gives the throttling threshold",
                float,
                DEFAULT_BYTES_THROTTLE_DIVIDER,
            ),
            Configuration(
                "threads_throttle_divider",
                "max threads divided by this number is the number of threads we use to execute queries for a throttled (project_id|organization_id, referrer)",
                int,
                DEFAULT_THREADS_THROTTLE_DIVIDER,
            ),
        ]

    def _are_tenant_ids_valid(self, tenant_ids: dict[str, str | int]) -> tuple[bool, str]:
        if self.is_cross_org_query(tenant_ids):
            return True, "cross org query"
        if tenant_ids.get("referrer") is None:
            return False, "no referrer"
        return True, ""

    def _get_customer_tenant_key_and_value(
        self, tenant_ids: dict[str, str | int]
    ) -> tuple[str, str | int]:
        # TODO: fold this into the above function
        if "project_id" in tenant_ids:
            return "project_id", tenant_ids["project_id"]
        if "organization_id" in tenant_ids:
            return "organization_id", tenant_ids["organization_id"]
        raise InvalidTenantsForAllocationPolicy.from_args(
            tenant_ids,
            self.__class__.__name__,
            "tenant_ids must include organization_id or project id",
        )

    def __get_window_tenant(
        self, tenant_ids: dict[str, str | int], referrer: str | int
    ) -> tuple[str, str | int, str]:
        """The (tenant key, tenant value, window prefix) this query counts against."""
        key_by = self.get_config_value("key_by")
        if key_by not in _KEY_BY_VALUES:
            logger.warning("Unknown key_by %s on %s", key_by, self.component_name())
            key_by = KEY_BY_TENANT_REFERRER
        if key_by != KEY_BY_TENANT_REFERRER and "organization_id" in tenant_ids:
            tenant_key: str = "organization_id"
            tenant_value = tenant_ids["organization_id"]
        else:
            tenant_key, tenant_value = self._get_customer_tenant_key_and_value(tenant_ids)
        prefix = f"{self.component_name()}-{tenant_key}-{tenant_value}"
        # The org-only window drops the referrer; the other two keep today's key so
        # existing windows carry over.
        if key_by != KEY_BY_ORGANIZATION or tenant_key != "organization_id":
            prefix = f"{prefix}-{referrer}"
        return tenant_key, tenant_value, prefix

    def __get_scan_limit(self, tenant_key: str) -> int:
        scan_limit = int(self.get_config_value("scan_limit"))
        if scan_limit != DEFAULT_OVERRIDE_LIMIT:
            return scan_limit
        if tenant_key == "project_id":
            return int(self.get_config_value("project_referrer_scan_limit"))
        return int(self.get_config_value("organization_referrer_scan_limit"))

    def __get_max_bytes_to_read(self) -> int | None:
        cap = self.get_config_value("max_bytes_to_read")
        if cap != DEFAULT_OVERRIDE_LIMIT:
            return int(cap)
        return None

    def _get_quota_allowance(
        self, tenant_ids: dict[str, str | int], query_id: str
    ) -> QuotaAllowance:
        ids_are_valid, why = self._are_tenant_ids_valid(tenant_ids)
        if not ids_are_valid:
            raise InvalidTenantsForAllocationPolicy.from_args(
                tenant_ids, self.__class__.__name__, why
            )
        if self.is_cross_org_query(tenant_ids):
            return QuotaAllowance(
                can_run=True,
                max_threads=self.max_threads,
                explanation={"reason": "cross_org_query"},
                is_throttled=False,
                throttle_threshold=MAX_THRESHOLD,
                rejection_threshold=MAX_THRESHOLD,
                quota_used=0,
                quota_unit=QUOTA_UNIT,
                suggestion=CROSS_ORG_SUGGESTION,
            )
        (
            customer_tenant_key,
            customer_tenant_value,
        ) = self._get_customer_tenant_key_and_value(tenant_ids)
        referrer = tenant_ids.get("referrer", "no_referrer")
        if referrer in _PASS_THROUGH_REFERRERS:
            return QuotaAllowance(
                can_run=True,
                max_threads=self.max_threads,
                explanation={},
                is_throttled=False,
                throttle_threshold=MAX_THRESHOLD,
                rejection_threshold=MAX_THRESHOLD,
                quota_used=0,
                quota_unit=QUOTA_UNIT,
                suggestion=PASS_THROUGH_REFERRERS_SUGGESTION,
            )

        if customer_tenant_key == "organization_id":
            org_cap = self.__get_max_bytes_to_read()
            if org_cap is not None:
                return QuotaAllowance(
                    can_run=True,
                    max_threads=self.max_threads,
                    max_bytes_to_read=org_cap,
                    explanation={
                        "reason": f"organization_id {customer_tenant_value} runs with a per-org max_bytes_to_read cap of {org_cap}"
                    },
                    is_throttled=False,
                    throttle_threshold=MAX_THRESHOLD,
                    rejection_threshold=MAX_THRESHOLD,
                    quota_used=0,
                    quota_unit=QUOTA_UNIT,
                    suggestion=NO_SUGGESTION,
                )

        tenant_key, tenant_value, window_prefix = self.__get_window_tenant(tenant_ids, referrer)
        scan_limit = self.__get_scan_limit(tenant_key)
        throttle_threshold = max(
            1, int(scan_limit // self.get_config_value("bytes_throttle_divider"))
        )
        timestamp, granted_quotas = _RATE_LIMITER.check_within_quotas(
            [
                RequestedQuota(
                    self.component_name(),
                    # request a big number because we don't know how much we actually
                    # will use in this query. this doesn't use up any quota, we just want to know how much is left
                    UNREASONABLY_LARGE_NUMBER_OF_BYTES_SCANNED_PER_QUERY,
                    [
                        Quota(
                            # TODO: Make window configurable but I don't know exactly how the rate limiter
                            # reacts to such changes
                            window_seconds=self.WINDOW_SECONDS,
                            granularity_seconds=self.WINDOW_GRANULARITY_SECONDS,
                            limit=scan_limit,
                            prefix_override=window_prefix,
                        )
                    ],
                ),
            ]
        )
        explanation: dict[str, Any] = {}
        granted_quota = granted_quotas[0]
        used_quota = scan_limit - granted_quota.granted
        if granted_quota.granted <= 0:
            explanation[
                "reason"
            ] = f"""{tenant_key} {tenant_value} is over the bytes scanned limit of {scan_limit} for referrer {referrer}.
                This policy is exceeded when a customer is abusing a specific feature in a way that puts load on clickhouse. If this is happening to
                "many customers, that may mean the feature is written in an inefficient way"""
            explanation["granted_quota"] = granted_quota.granted
            explanation["limit"] = scan_limit
            # This is technically a high cardinality tag value however these rejections
            # should not happen often therefore it should be safe to output these rejections as metris

            self.metrics.increment(
                "bytes_scanned_rejection",
                tags={"tenant": f"{tenant_key}__{tenant_value}__{referrer}"},
            )
            return QuotaAllowance(
                can_run=False,
                max_threads=0,
                explanation=explanation,
                is_throttled=True,
                throttle_threshold=throttle_threshold,
                rejection_threshold=scan_limit,
                quota_used=used_quota,
                quota_unit=QUOTA_UNIT,
                suggestion=SUGGESTION,
            )

        # this checks to see if you reached the throttle threshold
        if granted_quota.granted < scan_limit - throttle_threshold:
            self.metrics.increment(
                "bytes_scanned_queries_throttled",
                tags={"referrer": str(tenant_ids.get("referrer", "no_referrer"))},
            )
            return QuotaAllowance(
                can_run=True,
                max_threads=max(
                    1,
                    self.max_threads // self.get_config_value("threads_throttle_divider"),
                ),
                explanation={"reason": "within_limit but throttled"},
                is_throttled=True,
                throttle_threshold=throttle_threshold,
                rejection_threshold=scan_limit,
                quota_used=used_quota,
                quota_unit=QUOTA_UNIT,
                suggestion=SUGGESTION,
            )

        return QuotaAllowance(
            can_run=True,
            max_threads=self.max_threads,
            explanation={"reason": "within_limit"},
            is_throttled=False,
            throttle_threshold=throttle_threshold,
            rejection_threshold=scan_limit,
            quota_used=used_quota,
            quota_unit=QUOTA_UNIT,
            suggestion=NO_SUGGESTION,
        )

    def _get_bytes_scanned_in_query(
        self, tenant_ids: dict[str, str | int], result_or_error: QueryResultOrError
    ) -> int:
        if result_or_error.error:
            if (
                isinstance(result_or_error.error.__cause__, ClickhouseError)
                and result_or_error.error.__cause__.code == ErrorCodes.TIMEOUT_EXCEEDED
            ):
                return int(self.get_config_value("clickhouse_timeout_bytes_scanned_penalization"))
            return 0
        assert result_or_error.query_result is not None
        progress_bytes_scanned = cast(
            int,
            (result_or_error.query_result.result.get("profile") or {}).get("progress_bytes", None),
        )
        if isinstance(progress_bytes_scanned, (int, float)):
            self.metrics.increment(
                "progress_bytes_scanned",
                progress_bytes_scanned,
                tags={"referrer": str(tenant_ids.get("referrer", "no_referrer"))},
            )
        return progress_bytes_scanned

    def _update_quota_balance(
        self,
        tenant_ids: dict[str, str | int],
        query_id: str,
        result_or_error: QueryResultOrError,
    ) -> None:
        ids_are_valid, why = self._are_tenant_ids_valid(tenant_ids)
        if not ids_are_valid:
            # we already logged the reason before the query
            return
        if self.is_cross_org_query(tenant_ids):
            return
        bytes_scanned = self._get_bytes_scanned_in_query(tenant_ids, result_or_error)
        if bytes_scanned == 0:
            return
        referrer = tenant_ids.get("referrer", "no_referrer")
        (
            customer_tenant_key,
            customer_tenant_value,
        ) = self._get_customer_tenant_key_and_value(tenant_ids)
        # Mirror the bypass in _get_quota_allowance: org-keyed queries running under a
        # max_bytes_to_read cap skip the sliding window entirely. Recording usage here
        # would silently fill the window, so removing the cap later would reject queries
        # against quota they never actually consumed.
        if customer_tenant_key == "organization_id" and self.__get_max_bytes_to_read() is not None:
            return
        tenant_key, tenant_value, window_prefix = self.__get_window_tenant(tenant_ids, referrer)
        scan_limit = self.__get_scan_limit(tenant_key)
        # we can assume that the requested quota was granted (because it was)
        # we just need to update the quota with however many bytes were consumed
        _RATE_LIMITER.use_quotas(
            [
                RequestedQuota(
                    f"{self.component_name()}-{tenant_key}-{tenant_value}",
                    bytes_scanned,
                    [
                        Quota(
                            window_seconds=self.WINDOW_SECONDS,
                            granularity_seconds=self.WINDOW_GRANULARITY_SECONDS,
                            limit=scan_limit,
                            prefix_override=window_prefix,
                        )
                    ],
                )
            ],
            grants=[
                GrantedQuota(
                    f"{self.component_name()}-{tenant_key}-{tenant_value}",
                    granted=bytes_scanned,
                    reached_quotas=[],
                )
            ],
            timestamp=int(time.time()),
        )
