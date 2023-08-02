from __future__ import annotations

import logging

from snuba.query.allocation_policies import (
    AllocationPolicy,
    AllocationPolicyConfig,
    AllocationPolicyViolation,
    AllocationPolicyViolations,
    QueryResultOrError,
    QuotaAllowance,
)
from snuba.redis import RedisClientKey, get_redis_client
from snuba.state.rate_limit import (
    RateLimitParameters,
    rate_limit_finish_request,
    rate_limit_start_request,
)

DEFAULT_CONCURRENT_QUERIES_LIMIT = 22
DEFAULT_PER_SECOND_QUERIES_LIMIT = 50

rds = get_redis_client(RedisClientKey.RATE_LIMITER)

logger = logging.getLogger("snuba.query.allocation_policy_rate_limit")

_PASS_THROUGH_REFERRERS = set(
    [
        "subscriptions_executor",
    ]
)


class ConcurrentRateLimitAllocationPolicy(AllocationPolicy):
    def _additional_config_definitions(self) -> list[AllocationPolicyConfig]:
        # Define policy specific config definitions, these will be used along
        # with the default definitions of the base class. (is_enforced, is_active)
        return [
            AllocationPolicyConfig(
                name="concurrent_limit",
                description="maximum amount of concurrent queries per tenant",
                value_type=int,
                default=DEFAULT_CONCURRENT_QUERIES_LIMIT,
            ),
            AllocationPolicyConfig(
                name="rate_limit_shard_factor",
                description="""number of shards that each redis set is supposed to have.
                 increasing this value multiplies the number of redis keys by that
                 factor, and (on average) reduces the size of each redis set. You probably don't need to change this
                 unless you're scaling out redis for some reason
                 """,
                value_type=int,
                default=1,
            ),
        ]

    def _is_within_rate_limit(
        self, query_id: str, rate_limit_params: RateLimitParameters
    ) -> tuple[bool, str]:
        rate_limit_prefix = f"{self.runtime_config_prefix}.rate_limit"
        # HACK: this is a harcoded value because this rate_history_s is not a useful
        # configuration parameter. It's used for the per-second caclulation but that calculation
        # is fundamentally flawed
        rate_history_s = 1
        rate_limit_shard_factor = self.get_config_value("rate_limit_shard_factor")
        assert isinstance(rate_history_s, (int, float))
        assert isinstance(rate_limit_shard_factor, int)
        assert (
            rate_limit_params.concurrent_limit is not None
        ), "concurrent_limit must be set"

        assert rate_limit_shard_factor > 0

        rate_limit_stats = rate_limit_start_request(
            rate_limit_params,
            query_id,
            rate_history_s,
            rate_limit_shard_factor,
            rate_limit_prefix,
        )
        if rate_limit_stats.concurrent == -1:
            return True, "rate limiter errored, failing open"
        if rate_limit_stats.concurrent > rate_limit_params.concurrent_limit:
            return (
                False,
                f"concurrent policy {rate_limit_stats.concurrent} exceeds limit of {rate_limit_params.concurrent_limit}",
            )
        return True, "within limit"

    def _end_query(
        self,
        query_id: str,
        rate_limit_params: RateLimitParameters,
        result_or_error: QueryResultOrError,
    ) -> None:
        # removes the current query from the rate limit bookkeeping so it is no longer counted
        # in rate limits
        rate_limit_prefix = f"{self.runtime_config_prefix}.rate_limit"
        rate_limit_shard_factor = self.get_config_value("rate_limit_shard_factor")

        was_rate_limited = result_or_error.error is not None and isinstance(
            result_or_error.error.__cause__,
            (AllocationPolicyViolation, AllocationPolicyViolations),
        )
        rate_limit_finish_request(
            rate_limit_params,
            query_id,
            rate_limit_shard_factor,
            was_rate_limited,
            rate_limit_prefix,
        )

    def _get_tenant_key_and_value(
        self, tenant_ids: dict[str, str | int]
    ) -> tuple[str, str | int]:
        if "project_id" in tenant_ids:
            return "project_id", tenant_ids["project_id"]
        if "organization_id" in tenant_ids:
            return "organization_id", tenant_ids["organization_id"]
        raise AllocationPolicyViolation(
            "Queries must have a project id or organization id"
        )

    def _get_quota_allowance(
        self, tenant_ids: dict[str, str | int], query_id: str
    ) -> QuotaAllowance:
        if tenant_ids.get("referrer", "no_referrer") in _PASS_THROUGH_REFERRERS:
            return QuotaAllowance(
                can_run=True,
                max_threads=self.max_threads,
                explanation={"reason": "pass_through"},
            )
        tenant_key, tenant_value = self._get_tenant_key_and_value(tenant_ids)
        within_rate_limit, why = self._is_within_rate_limit(
            query_id,
            RateLimitParameters(
                tenant_key,
                bucket=str(tenant_value),
                per_second_limit=None,
                concurrent_limit=self.get_config_value("concurrent_limit"),
            ),
        )
        return QuotaAllowance(within_rate_limit, self.max_threads, {"reason": why})

    def _update_quota_balance(
        self,
        tenant_ids: dict[str, str | int],
        query_id: str,
        result_or_error: QueryResultOrError,
    ) -> None:
        tenant_key, tenant_value = self._get_tenant_key_and_value(tenant_ids)
        rate_limit_params = RateLimitParameters(
            tenant_key,
            bucket=str(tenant_value),
            per_second_limit=None,
            concurrent_limit=self.get_config_value("concurrent_limit"),
        )
        self._end_query(query_id, rate_limit_params, result_or_error)
