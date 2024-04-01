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
    InvalidTenantsForAllocationPolicy,
    QueryResultOrError,
    QuotaAllowance,
)
from snuba.redis import RedisClientKey, get_redis_client

logger = logging.getLogger("snuba.query.bytes_scanned_window_policy")


# we don't limit the amount of bytes subscriptions can scan at this time
_PASS_THROUGH_REFERRERS = set(
    [
        "subscriptions_executor",
    ]
)


UNREASONABLY_LARGE_NUMBER_OF_BYTES_SCANNED_PER_QUERY = int(1e12)
_RATE_LIMITER = RedisSlidingWindowRateLimiter(
    get_redis_client(RedisClientKey.RATE_LIMITER)
)
DEFAULT_OVERRIDE_LIMIT = -1
DEFAULT_BYTES_SCANNED_LIMIT = 10000000


class BytesScannedRejectingPolicy(AllocationPolicy):
    """Look at the amount of bytes a customer (either a project or an organization) has scanned for a specific referrer,
    reject queries if over the limit
    """

    WINDOW_SECONDS = 10 * 60
    WINDOW_GRANULARITY_SECONDS = 60

    def _additional_config_definitions(self) -> list[AllocationPolicyConfig]:
        # Overrides are prioritized in order of specificity.
        # If two overrides applicable available to the request, the one with a smaller value takes precedence
        return [
            AllocationPolicyConfig(
                "project_referrer_scan_limit_override",
                f"how many bytes can a specific project scan per referrer in the last {self.WINDOW_SECONDS/ 60} mins before queries start getting rejected",
                int,
                DEFAULT_BYTES_SCANNED_LIMIT,
                param_types={"project_id": int, "referrer": str},
            ),
            AllocationPolicyConfig(
                "organization_referrer_scan_limit_override",
                f"how many bytes can a specific organization scan per referrer in the last {self.WINDOW_SECONDS/ 60} mins before queries start getting rejected",
                int,
                DEFAULT_BYTES_SCANNED_LIMIT * 2,
                param_types={"organization_id": int, "referrer": str},
            ),
            AllocationPolicyConfig(
                "referrer_all_projects_scan_limit_override",
                f"Specific referrer scan limit in the last {self.WINDOW_SECONDS/ 60} mins, APPLIES TO ALL PROJECTS",
                int,
                DEFAULT_BYTES_SCANNED_LIMIT,
                param_types={"referrer": str},
            ),
            AllocationPolicyConfig(
                "referrer_all_organizations_scan_limit_override",
                f"Specific referrer scan limit in the last {self.WINDOW_SECONDS/ 60} mins, APPLIES TO ALL ORGANIZATIONS",
                int,
                DEFAULT_BYTES_SCANNED_LIMIT * 2,
                param_types={"referrer": str},
            ),
            AllocationPolicyConfig(
                "project_referrer_scan_limit",
                f"DEFAULT: how many bytes can a project scan per referrer in the last {self.WINDOW_SECONDS/ 60} mins before queries start getting rejected",
                int,
                DEFAULT_BYTES_SCANNED_LIMIT,
            ),
            AllocationPolicyConfig(
                "organization_referrer_scan_limit",
                f"DEFAULT: how many bytes can an organization scan per referrer in the last {self.WINDOW_SECONDS/ 60} mins before queries start getting rejected. Cross-project queries are limited by organization_id",
                int,
                DEFAULT_BYTES_SCANNED_LIMIT * 2,
            ),
        ]

    def _are_tenant_ids_valid(
        self, tenant_ids: dict[str, str | int]
    ) -> tuple[bool, str]:
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

    def _get_overrides(self, tenant_ids: dict[str, str | int]) -> dict[str, int]:
        overrides = {}
        available_tenant_ids = set(tenant_ids.keys())
        # get all overrides that can be retrieved with the tenant_ids
        # e.g. if organization_id and referrer are passed in, retrieve
        # ('organization_override, 'referrer_organization_override')
        for config_definition in self._additional_config_definitions():
            if config_definition.name.endswith("_override"):
                param_types = config_definition.param_types
                if set(param_types.keys()).issubset(available_tenant_ids):
                    params = {param: tenant_ids[param] for param in param_types}
                    config_value = self.get_config_value(config_definition.name, params)
                    if config_value != config_definition.default:
                        key = "|".join(
                            [
                                f"{param}__{tenant_id}"
                                for param, tenant_id in sorted(params.items())
                            ]
                        )

                        overrides[key] = config_value
        return overrides

    def __get_scan_limit(
        self,
        customer_tenant_key: str,
        customer_tenant_value: str | int,
        referrer: str | int,
    ) -> int:
        overrides = self._get_overrides(
            {customer_tenant_key: customer_tenant_value, "referrer": referrer}
        )
        if overrides:
            raise NotImplementedError
        if customer_tenant_key == "project_id":
            return self.get_config_value("project_referrer_scan_limit")
        elif customer_tenant_key == "organization_id":
            return self.get_config_value("organization_referrer_scan_limit")
        raise NotImplementedError

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
            )
        (
            customer_tenant_key,
            customer_tenant_value,
        ) = self._get_customer_tenant_key_and_value(tenant_ids)
        referrer = tenant_ids.get("referrer", "no_referrer")
        if referrer in _PASS_THROUGH_REFERRERS:
            return QuotaAllowance(True, self.max_threads, {})
        scan_limit = self.__get_scan_limit(
            customer_tenant_key, customer_tenant_value, referrer
        )

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
                            limit=scan_limit,
                            prefix_override=f"{self.runtime_config_prefix}-{customer_tenant_key}-{customer_tenant_value}-{referrer}",
                        )
                    ],
                ),
            ]
        )
        explanation: dict[str, Any] = {}
        granted_quota = granted_quotas[0]
        if granted_quota.granted <= 0:
            explanation[
                "reason"
            ] = f"{customer_tenant_key} {customer_tenant_value} is over the bytes scanned limit of {scan_limit} for referrer {referrer}"
            explanation["granted_quota"] = granted_quota.granted
            explanation["limit"] = scan_limit
            return QuotaAllowance(False, self.max_threads, explanation)
        return QuotaAllowance(True, self.max_threads, {"reason": "within_limit"})

    def _get_bytes_scanned_in_query(
        self, tenant_ids: dict[str, str | int], result_or_error: QueryResultOrError
    ) -> int:
        progress_bytes_scanned = cast(int, result_or_error.query_result.result.get("profile", {}).get("progress_bytes", None))  # type: ignore
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
        if result_or_error.error:
            return
        bytes_scanned = self._get_bytes_scanned_in_query(tenant_ids, result_or_error)
        query_result = result_or_error.query_result
        assert query_result is not None
        if bytes_scanned is None:
            logging.error("No bytes scanned in query_result %s", query_result)
            return
        if bytes_scanned == 0:
            return
        referrer = tenant_ids.get("referrer", "no_referrer")
        (
            customer_tenant_key,
            customer_tenant_value,
        ) = self._get_customer_tenant_key_and_value(tenant_ids)
        scan_limit = self.__get_scan_limit(
            customer_tenant_key, customer_tenant_value, referrer
        )
        # we can assume that the requested quota was granted (because it was)
        # we just need to update the quota with however many bytes were consumed
        _RATE_LIMITER.use_quotas(
            [
                RequestedQuota(
                    f"{self.runtime_config_prefix}-{customer_tenant_key}-{customer_tenant_value}",
                    bytes_scanned,
                    [
                        Quota(
                            window_seconds=self.WINDOW_SECONDS,
                            granularity_seconds=self.WINDOW_GRANULARITY_SECONDS,
                            limit=scan_limit,
                            prefix_override=f"{self.runtime_config_prefix}-{customer_tenant_key}-{customer_tenant_value}-{referrer}",
                        )
                    ],
                )
            ],
            grants=[
                GrantedQuota(
                    f"{self.runtime_config_prefix}-{customer_tenant_key}-{customer_tenant_value}",
                    granted=bytes_scanned,
                    reached_quotas=[],
                )
            ],
            timestamp=int(time.time()),
        )
