from __future__ import annotations

import logging
from typing import Callable, cast

from snuba import state
from snuba.query.allocation_policies import (
    AllocationPolicy,
    AllocationPolicyConfig,
    AllocationPolicyViolations,
    InvalidTenantsForAllocationPolicy,
    QueryResultOrError,
    QuotaAllowance,
)
from snuba.state.rate_limit import (
    RateLimitParameters,
    rate_limit_finish_request,
    rate_limit_start_request,
)

DEFAULT_CONCURRENT_QUERIES_LIMIT = 22
DEFAULT_PER_SECOND_QUERIES_LIMIT = 50

logger = logging.getLogger("snuba.query.allocation_policy_rate_limit")

_PASS_THROUGH_REFERRERS = set(
    [
        "subscriptions_executor",
    ]
)


class BaseConcurrentRateLimitAllocationPolicy(AllocationPolicy):
    def _additional_config_definitions(self) -> list[AllocationPolicyConfig]:
        return [
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
            AllocationPolicyConfig(
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
            self.get_config_value("max_query_duration_s"),
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
    def _additional_config_definitions(self) -> list[AllocationPolicyConfig]:
        return super()._additional_config_definitions() + [
            AllocationPolicyConfig(
                name="concurrent_limit",
                description="maximum amount of concurrent queries per tenant",
                value_type=int,
                default=DEFAULT_CONCURRENT_QUERIES_LIMIT,
            ),
            AllocationPolicyConfig(
                name="referrer_project_override",
                description="override concurrent limit for a specific project, referrer combo",
                value_type=int,
                default=-1,
                param_types={"referrer": str, "project_id": int},
            ),
            AllocationPolicyConfig(
                name="referrer_organization_override",
                description="override concurrent limit for a specific organization_id, referrer combo",
                value_type=int,
                default=-1,
                param_types={"referrer": str, "organization_id": int},
            ),
            AllocationPolicyConfig(
                name="project_override",
                description="override concurrent limit for a specific project_id",
                value_type=int,
                default=-1,
                param_types={"project_id": int},
            ),
            AllocationPolicyConfig(
                name="organization_override",
                description="override concurrent limit for a specific organization_id",
                value_type=int,
                default=-1,
                param_types={"organization_id": int},
            ),
        ]

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

    def _get_tenant_key_and_value(
        self, tenant_ids: dict[str, str | int]
    ) -> tuple[str, str | int]:
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

    def _get_rate_limit_params(
        self, tenant_ids: dict[str, str | int]
    ) -> tuple[RateLimitParameters, dict[str, int]]:
        tenant_key, tenant_value = self._get_tenant_key_and_value(tenant_ids)
        overrides = self._get_overrides(tenant_ids)
        concurrent_limit = self.get_config_value("concurrent_limit")
        if overrides:
            concurrent_limit = min(overrides.values())
            tenant_value = min(overrides, key=cast(Callable[[str], int], overrides.get))

        return (
            RateLimitParameters(
                self.rate_limit_name,
                bucket=str(tenant_value),
                per_second_limit=None,
                concurrent_limit=concurrent_limit,
            ),
            overrides,
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
        if self.is_cross_org_query(tenant_ids):
            return QuotaAllowance(
                can_run=True,
                max_threads=self.max_threads,
                explanation={"reason": "cross_org"},
            )

        rate_limit_params, overrides = self._get_rate_limit_params(tenant_ids)
        within_rate_limit, why = self._is_within_rate_limit(query_id, rate_limit_params)
        return QuotaAllowance(
            within_rate_limit, self.max_threads, {"reason": why, "overrides": overrides}
        )

    def _update_quota_balance(
        self,
        tenant_ids: dict[str, str | int],
        query_id: str,
        result_or_error: QueryResultOrError,
    ) -> None:
        if self.is_cross_org_query(tenant_ids):
            return
        rate_limit_params, _ = self._get_rate_limit_params(tenant_ids)
        self._end_query(query_id, rate_limit_params, result_or_error)
