from __future__ import annotations

import logging
from typing import Any, cast

from snuba.datasets.storages.storage_key import StorageKey
from snuba.query.allocation_policies import (
    AllocationPolicyConfig,
    QueryResultOrError,
    QuotaAllowance,
)
from snuba.query.allocation_policies.concurrent_rate_limit import (
    BaseConcurrentRateLimitAllocationPolicy,
)
from snuba.redis import RedisClientKey, get_redis_client
from snuba.state.rate_limit import RateLimitParameters

DEFAULT_CONCURRENT_QUERIES_LIMIT = 22
DEFAULT_PER_SECOND_QUERIES_LIMIT = 50

rds = get_redis_client(RedisClientKey.RATE_LIMITER)

logger = logging.getLogger("snuba.query.allocation_policy_cross_org")

_PASS_THROUGH_REFERRERS = set(
    [
        "subscriptions_executor",
    ]
)

_RATE_LIMIT_NAME = "concurrent_limit_policy"


class CrossOrgQueryAllocationPolicy(BaseConcurrentRateLimitAllocationPolicy):
    @property
    def rate_limit_name(self) -> str:
        return "cross_org_query_policy"

    def _additional_config_definitions(self) -> list[AllocationPolicyConfig]:
        return super()._additional_config_definitions() + [
            AllocationPolicyConfig(
                name="referrer_concurrent_override",
                description="""override the concurrent limit for a referrer""",
                value_type=int,
                param_types={"referrer": str},
                default=-1,
            ),
            AllocationPolicyConfig(
                name="referrer_max_threads_override",
                description="""override the max_threads for a referrer, applies to every query made by that referrer""",
                param_types={"referrer": str},
                value_type=int,
                default=-1,
            ),
        ]

    def _validate_cross_org_referrer_limits(
        self, cross_org_referrer_limits: dict[str, dict[str, int]]
    ) -> None:
        for referrer, limits in cross_org_referrer_limits.items():
            concurrent_limit = limits.get("concurrent_limit", None)
            max_threads = limits.get("max_threads", None)
            if not isinstance(max_threads, int):
                raise ValueError(
                    f"max_threads is required for {referrer} and must be an int"
                )
            if not isinstance(concurrent_limit, int):
                raise ValueError(
                    f"concurrent_limit is required for {referrer} and must be an int"
                )

    def __init__(
        self,
        storage_key: StorageKey,
        required_tenant_types: list[str],
        default_config_overrides: dict[str, Any],
        **kwargs: str,
    ) -> None:
        super().__init__(
            storage_key, required_tenant_types, default_config_overrides, **kwargs
        )
        self._registered_cross_org_referrers = cast(
            "dict[str, dict[str, int]]", kwargs.get("cross_org_referrer_limits", {})
        )
        self._validate_cross_org_referrer_limits(self._registered_cross_org_referrers)

    def _get_quota_allowance(
        self, tenant_ids: dict[str, str | int], query_id: str
    ) -> QuotaAllowance:
        referrer = str(tenant_ids.get("referrer", "no_referrer"))
        referrer_is_registered = referrer in self._registered_cross_org_referrers

        if not referrer_is_registered and tenant_ids.get("cross_org_query", False):
            return QuotaAllowance(
                can_run=False,
                max_threads=0,
                explanation={
                    "reason": f"cross_org_query is passed as a tenant but referrer {referrer} is not registered, update the {self._storage_key.value} yaml config to register this referrer"
                },
            )
        elif not referrer_is_registered:
            # This is not a cross org query and the referrer is not registered. This is outside the responsibility of this policy
            return QuotaAllowance(
                can_run=True,
                max_threads=self.max_threads,
                explanation={"reason": "pass_through"},
            )

        thread_override = self.get_config_value(
            "referrer_max_threads_override", {"referrer": referrer}
        )

        max_threads = (
            thread_override
            if thread_override != -1
            else int(self._registered_cross_org_referrers[referrer]["max_threads"])
        )

        concurrent_override = self.get_config_value(
            "referrer_concurrent_override", {"referrer": referrer}
        )
        concurrent_limit = (
            concurrent_override
            if concurrent_override != -1
            else int(self._registered_cross_org_referrers[referrer]["concurrent_limit"])
        )
        can_run, explanation = self._is_within_rate_limit(
            query_id,
            RateLimitParameters(self.rate_limit_name, referrer, None, concurrent_limit),
        )
        return QuotaAllowance(
            can_run=can_run,
            max_threads=max_threads,
            explanation={"reason": explanation},
        )

    def _update_quota_balance(
        self,
        tenant_ids: dict[str, str | int],
        query_id: str,
        result_or_error: QueryResultOrError,
    ) -> None:
        referrer = str(tenant_ids.get("referrer", "no_referrer"))
        rate_limit_params = RateLimitParameters(
            self.rate_limit_name,
            referrer,
            None,
            # limit number does not matter for ending a query so I just picked 22
            22,
        )
        self._end_query(query_id, rate_limit_params, result_or_error)
