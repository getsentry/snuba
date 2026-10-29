from __future__ import annotations

import logging
import typing

from snuba import state
from snuba.configs.configuration import Configuration
from snuba.query.allocation_policies import (
    CROSS_ORG_SUGGESTION,
    MAX_THRESHOLD,
    NO_SUGGESTION,
    PASS_THROUGH_REFERRERS_SUGGESTION,
    AllocationPolicy,
    AllocationPolicyViolations,
    InvalidTenantsForAllocationPolicy,
    QueryResultOrError,
    QueryType,
    QuotaAllowance,
)
from snuba.state.rate_limit import (
    RateLimitParameters,
    RateLimitStats,
    rate_limit_finish_request,
    rate_limit_start_request,
)

DEFAULT_CONCURRENT_QUERIES_LIMIT = 22
DEFAULT_PER_SECOND_QUERIES_LIMIT = 50

logger = logging.getLogger("snuba.query.allocation_policy_rate_limit")

_PASS_THROUGH_REFERRERS = {
    # these referrers are tied to ingest and are better limited by the ReferrerGuardRailPolicy
    "subscriptions_executor",
    "tsdb-modelid:4.batch_alert_event_frequency",
    "tsdb-modelid:4.batch_alert_event_uniq_user_frequency",
    "tsdb-modelid:4.batch_alert_event_frequency_percent",
    "tsdb-modelid:4.wf_batch_alert_event_frequency",
    "tsdb-modelid:300.wf_batch_alert_event_uniq_user_frequency",
    "tsdb-modelid:4.wf_batch_alert_event_frequency_percent",
}

QUOTA_UNIT = "concurrent_queries"
KEY_BY_TENANT = "tenant"
KEY_BY_ORGANIZATION = "organization"
KEY_BY_ORGANIZATION_REFERRER = "organization_referrer"
_KEY_BY_VALUES = {KEY_BY_TENANT, KEY_BY_ORGANIZATION, KEY_BY_ORGANIZATION_REFERRER}
# Never configured in any region. Changing it would move every count to new Redis keys.
_RATE_LIMIT_SHARD_FACTOR = 1
SUGGESTION = "A customer is sending too many queries to snuba. The customer may be abusing an API or the queries may be innefficient"


class BaseConcurrentRateLimitAllocationPolicy(AllocationPolicy):
    def _additional_config_definitions(self) -> list[Configuration]:
        return [
            Configuration(
                name="max_query_duration_s",
                description="""maximum duration of a query in seconds. Queries that exceed this duration  are considered finished by the rate limiter. This reduces memory usage. If you turn this down lower than the actual timeout period, the system can start undercounting concurrent queries""",
                value_type=int,
                default=state.max_query_duration_s,
            ),
        ]

    @property
    def rate_limit_name(self) -> str:
        raise NotImplementedError

    def _is_within_rate_limit(
        self, query_id: str, rate_limit_params: RateLimitParameters
    ) -> tuple[RateLimitStats, bool, str]:
        rate_limit_prefix = f"{self.component_name()}.rate_limit"
        # HACK: this is a harcoded value because this rate_history_s is not a useful
        # configuration parameter. It's used for the per-second caclulation but that calculation
        # is fundamentally flawed
        rate_history_s = 1
        assert isinstance(rate_history_s, (int, float))
        assert rate_limit_params.concurrent_limit is not None, "concurrent_limit must be set"

        rate_limit_stats = rate_limit_start_request(
            rate_limit_params,
            query_id,
            rate_history_s,
            _RATE_LIMIT_SHARD_FACTOR,
            rate_limit_prefix,
            self.get_config_value("max_query_duration_s"),
        )
        if rate_limit_stats.concurrent == -1:
            return rate_limit_stats, True, "rate limiter errored, failing open"
        if rate_limit_stats.concurrent > rate_limit_params.concurrent_limit:
            return (
                rate_limit_stats,
                False,
                f"concurrent policy {rate_limit_stats.concurrent} exceeds limit of {rate_limit_params.concurrent_limit}",
            )
        return rate_limit_stats, True, "within limit"

    def _end_query(
        self,
        query_id: str,
        rate_limit_params: RateLimitParameters,
        result_or_error: QueryResultOrError,
    ) -> None:
        # removes the current query from the rate limit bookkeeping so it is no longer counted
        # in rate limits
        rate_limit_prefix = f"{self.component_name()}.rate_limit"
        was_rate_limited = result_or_error.error is not None and isinstance(
            result_or_error.error.__cause__,
            AllocationPolicyViolations,
        )
        rate_limit_finish_request(
            rate_limit_params,
            query_id,
            _RATE_LIMIT_SHARD_FACTOR,
            was_rate_limited,
            rate_limit_prefix,
            self.get_config_value("max_query_duration_s"),
        )


class ConcurrentRateLimitAllocationPolicy(BaseConcurrentRateLimitAllocationPolicy):
    def _additional_config_definitions(self) -> list[Configuration]:
        return super()._additional_config_definitions() + [
            Configuration(
                name="concurrent_limit",
                description="maximum amount of concurrent queries per tenant",
                value_type=int,
                default=DEFAULT_CONCURRENT_QUERIES_LIMIT,
            ),
            Configuration(
                name="key_by",
                description=f"Which tenant the concurrency count is kept for: {KEY_BY_TENANT} (project_id if present, else organization_id), {KEY_BY_ORGANIZATION} or {KEY_BY_ORGANIZATION_REFERRER}. Per-tenant limits go in match blocks. Queries without an organization_id use {KEY_BY_TENANT}",
                value_type=str,
                default=KEY_BY_TENANT,
            ),
        ]

    def _get_tenant_key_and_value(self, tenant_ids: dict[str, str | int]) -> tuple[str, str | int]:
        if "project_id" in tenant_ids:
            return "project_id", tenant_ids["project_id"]
        if "organization_id" in tenant_ids:
            return "organization_id", tenant_ids["organization_id"]
        raise InvalidTenantsForAllocationPolicy.from_args(
            tenant_ids,
            self.__class__.__name__,
            "tenant_ids must include organization_id or project id",
        )

    @property
    def rate_limit_name(self) -> str:
        return "concurrent_rate_limit_policy"

    def _get_rate_limit_params(self, tenant_ids: dict[str, str | int]) -> RateLimitParameters:
        return RateLimitParameters(
            self.rate_limit_name,
            bucket=self._get_bucket(tenant_ids),
            per_second_limit=None,
            concurrent_limit=self.get_config_value("concurrent_limit"),
        )

    def _get_bucket(self, tenant_ids: dict[str, str | int]) -> str:
        key_by = self.get_config_value("key_by")
        if key_by not in _KEY_BY_VALUES:
            logger.warning("Unknown key_by %s on %s", key_by, self.component_name())
            key_by = KEY_BY_TENANT
        if key_by != KEY_BY_TENANT and "organization_id" in tenant_ids:
            # Same bucket names the removed organization_override and
            # referrer_organization_override params used.
            bucket = f"organization_id__{tenant_ids['organization_id']}"
            if key_by == KEY_BY_ORGANIZATION_REFERRER:
                bucket += f"|referrer__{tenant_ids.get('referrer', 'no_referrer')}"
            return bucket
        _, tenant_value = self._get_tenant_key_and_value(tenant_ids)
        return str(tenant_value)

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

        rate_limit_params = self._get_rate_limit_params(tenant_ids)
        rate_limit_stats, within_rate_limit, why = self._is_within_rate_limit(
            query_id,
            rate_limit_params,
        )

        return QuotaAllowance(
            can_run=within_rate_limit,
            max_threads=self.max_threads if within_rate_limit else 0,
            explanation={"reason": why},
            is_throttled=False,
            throttle_threshold=typing.cast(int, rate_limit_params.concurrent_limit),
            rejection_threshold=typing.cast(int, rate_limit_params.concurrent_limit),
            quota_used=rate_limit_stats.concurrent,
            quota_unit=QUOTA_UNIT,
            suggestion=NO_SUGGESTION if within_rate_limit else SUGGESTION,
        )

    def _update_quota_balance(
        self,
        tenant_ids: dict[str, str | int],
        query_id: str,
        result_or_error: QueryResultOrError,
    ) -> None:
        if self.is_cross_org_query(tenant_ids):
            return
        rate_limit_params = self._get_rate_limit_params(tenant_ids)
        self._end_query(query_id, rate_limit_params, result_or_error)


class DeleteConcurrentRateLimitAllocationPolicy(ConcurrentRateLimitAllocationPolicy):
    @property
    def rate_limit_name(self) -> str:
        return "delete_concurrent_rate_limit_policy"

    @property
    def query_type(self) -> QueryType:
        return QueryType.DELETE
