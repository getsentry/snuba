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

_RATE_LIMIT_NAME = "concurrent_limit_policy"
_UNREGISTERED_REFERRER_MAX_THREADS = 1
_UNREGISTERED_REFERRER_CONCURRENT_QUERIES = 1


class CrossOrgQueryAllocationPolicy(BaseConcurrentRateLimitAllocationPolicy):
    """A case-by-case allocation policy for cross-org queries. All referrers affected by this policy have to be registered
    in this class's configuration through the `cross_org_referrer_limits` parameter. Example:

    ```yaml
        - name: CrossOrgQueryAllocationPolicy
          args:
            required_tenant_types:
              - referrer
            default_config_overrides:
              is_enforced: 0
              is_active: 0
            cross_org_referrer_limits:
              dynamic_sampling.counters.get_org_transaction_volumes:
                max_threads: 4
                concurrent_limit: 10
    ```

    Each referrer gets a concurrent limit (applied per referrer) and a max_threads limit (applied to every query made by that referrer).
    Both limits are static but changeable at runtime.

    unregistered referrers are assigned a default concurrent limit and max_threads limit of 1.
    """

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

    def _get_max_threads(self, referrer: str) -> int:
        if not self._referrer_is_registered(referrer):
            return _UNREGISTERED_REFERRER_MAX_THREADS
        thread_override = int(
            self.get_config_value(
                "referrer_max_threads_override", {"referrer": referrer}
            )
        )
        return (
            thread_override
            if thread_override != -1
            else int(self._registered_cross_org_referrers[referrer]["max_threads"])
        )

    def _get_concurrent_limit(self, referrer: str) -> int:
        if not self._referrer_is_registered(referrer):
            return _UNREGISTERED_REFERRER_CONCURRENT_QUERIES
        concurrent_override = int(
            self.get_config_value(
                "referrer_concurrent_override", {"referrer": referrer}
            )
        )
        return (
            concurrent_override
            if concurrent_override != -1
            else int(self._registered_cross_org_referrers[referrer]["concurrent_limit"])
        )

    def _referrer_is_registered(self, referrer: str) -> bool:
        return referrer in self._registered_cross_org_referrers

    def _get_quota_allowance(
        self, tenant_ids: dict[str, str | int], query_id: str
    ) -> QuotaAllowance:
        referrer = str(tenant_ids.get("referrer", "no_referrer"))

        if not self._referrer_is_registered(referrer) and not self.is_cross_org_query(
            tenant_ids
        ):
            # This is not a cross org query and the referrer is not registered. This is outside the responsibility of this policy
            return QuotaAllowance(
                can_run=True,
                max_threads=self.max_threads,
                explanation={"reason": "pass_through"},
            )

        concurrent_limit = self._get_concurrent_limit(referrer)
        can_run, explanation = self._is_within_rate_limit(
            query_id,
            RateLimitParameters(self.rate_limit_name, referrer, None, concurrent_limit),
        )
        return QuotaAllowance(
            can_run=can_run,
            max_threads=self._get_max_threads(referrer),
            explanation={"reason": explanation},
        )

    def _update_quota_balance(
        self,
        tenant_ids: dict[str, str | int],
        query_id: str,
        result_or_error: QueryResultOrError,
    ) -> None:
        referrer = str(tenant_ids.get("referrer", "no_referrer"))
        if not self._referrer_is_registered(referrer) and not self.is_cross_org_query(
            tenant_ids
        ):
            return
        rate_limit_params = RateLimitParameters(
            self.rate_limit_name,
            referrer,
            None,
            # limit number does not matter for ending a query so I just picked 22
            22,
        )
        self._end_query(query_id, rate_limit_params, result_or_error)
