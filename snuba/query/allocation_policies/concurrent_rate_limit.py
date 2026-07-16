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
SUGGESTION = "A customer is sending too many queries to snuba. The customer may be abusing an API or the queries may be innefficient"


class BaseConcurrentRateLimitAllocationPolicy(AllocationPolicy):
    def _additional_config_definitions(self) -> list[Configuration]:
        return [
            Configuration(
                name="rate_limit_shard_factor",
                description="""number of shards that each redis set is supposed to have.
                 increasing this value multiplies the number of redis keys by that
                 factor, and (on average) reduces the size of each redis set. You probably don't need to change this
                 unless you're scaling out redis for some reason
                 """,
                value_type=int,
                default=1,
            ),
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
        rate_limit_shard_factor = self.get_config_value("rate_limit_shard_factor")
        assert isinstance(rate_history_s, (int, float))
        assert isinstance(rate_limit_shard_factor, int)
        assert rate_limit_params.concurrent_limit is not None, "concurrent_limit must be set"

        assert rate_limit_shard_factor > 0

        rate_limit_stats = rate_limit_start_request(
            rate_limit_params,
            query_id,
            rate_history_s,
            rate_limit_shard_factor,
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
        rate_limit_shard_factor = self.get_config_value("rate_limit_shard_factor")

        was_rate_limited = result_or_error.error is not None and isinstance(
            result_or_error.error.__cause__,
            AllocationPolicyViolations,
        )
        rate_limit_finish_request(
            rate_limit_params,
            query_id,
            rate_limit_shard_factor,
            was_rate_limited,
            rate_limit_prefix,
            self.get_config_value("max_query_duration_s"),
        )


class ConcurrentRateLimitAllocationPolicy(BaseConcurrentRateLimitAllocationPolicy):
    def _additional_config_definitions(self) -> list[Configuration]:
        return super()._additional_config_definitions() + [
            Configuration(
                name="concurrent_limit",
                description=(
                    "maximum amount of concurrent queries per tenant. Scopable per "
                    "project/organization and/or referrer via the override option; the "
                    "most-specific matching value wins."
                ),
                value_type=int,
                default=DEFAULT_CONCURRENT_QUERIES_LIMIT,
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
        tenant_key, tenant_value = self._get_tenant_key_and_value(tenant_ids)
        referrer = tenant_ids.get("referrer")
        # Most-specific-matching concurrent limit for this (tenant, referrer).
        concurrent_limit = int(self.get_config_value("concurrent_limit", tenant_ids))
        # Bucket by tenant so all referrers share the tenant-wide limit, unless a
        # referrer-specific override applies -- then count that referrer on its own
        # so it neither consumes nor is consumed by the tenant-wide bucket.
        bucket = str(tenant_value)
        if referrer is not None:
            tenant_wide_limit = int(
                self.get_config_value("concurrent_limit", {tenant_key: tenant_value})
            )
            if concurrent_limit != tenant_wide_limit:
                bucket = f"{tenant_value}|referrer__{referrer}"
        return RateLimitParameters(
            self.rate_limit_name,
            bucket=bucket,
            per_second_limit=None,
            concurrent_limit=concurrent_limit,
        )

    def _get_quota_allowance(
        self, tenant_ids: dict[str, str | int], query_id: str
    ) -> QuotaAllowance:
        if tenant_ids.get("referrer", "no_referrer") in _PASS_THROUGH_REFERRERS:
            return QuotaAllowance(
                can_run=True,
                max_threads=self._max_threads(tenant_ids),
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
                max_threads=self._max_threads(tenant_ids),
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
            max_threads=self._max_threads(tenant_ids) if within_rate_limit else 0,
            explanation={"reason": why, "concurrent_limit": rate_limit_params.concurrent_limit},
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
