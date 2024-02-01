from __future__ import annotations

import logging

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

logger = logging.getLogger("snuba.query.allocation_policy_per_referrer")

_DEFAULT_MAX_THREADS = 10
_PASS_THROUGH_REFERRERS = set(
    [
        "subscriptions_executor",
    ]
)


class ReferrerGuardRailPolicy(BaseConcurrentRateLimitAllocationPolicy):
    """
    A policy to prevent runaway referrers from consuming too many queries. This concern is orthogonal to customer rate limits.

    For example, a product team may push out a feature that sends 20 snuba queries every 5 seconds on the UI. In that case, that feature should break,
    not all of clickhouse.

    """

    @property
    def rate_limit_name(self) -> str:
        return "referrer_guard_rail_policy"

    def _additional_config_definitions(self) -> list[AllocationPolicyConfig]:
        return super()._additional_config_definitions() + [
            AllocationPolicyConfig(
                name="default_concurrent_request_per_referrer",
                description="""how many concurrent requests does a referrer get by default? This is set to a pretty high number.
                If every referrer did this number of concurrent queries we would not have enough capacity
                """,
                value_type=int,
                param_types={},
                default=100,
            ),
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

    def _get_max_threads(self, referrer: str) -> int:
        thread_override = int(
            self.get_config_value(
                "referrer_max_threads_override", {"referrer": referrer}
            )
        )
        return thread_override if thread_override != -1 else _DEFAULT_MAX_THREADS

    def _get_concurrent_limit(self, referrer: str) -> int:
        concurrent_override = int(
            self.get_config_value(
                "referrer_concurrent_override", {"referrer": referrer}
            )
        )
        default_concurrent_value = int(
            self.get_config_value("default_concurrent_request_per_referrer")
        )
        return (
            concurrent_override
            if concurrent_override != -1
            else default_concurrent_value
        )

    def _get_quota_allowance(
        self, tenant_ids: dict[str, str | int], query_id: str
    ) -> QuotaAllowance:
        referrer = str(tenant_ids.get("referrer", "no_referrer"))
        if referrer in _PASS_THROUGH_REFERRERS:
            return QuotaAllowance(
                True,
                _DEFAULT_MAX_THREADS,
                {"reason": f"{referrer} is a pass through referrer"},
            )
        concurrent_limit = self._get_concurrent_limit(referrer)
        can_run, explanation = self._is_within_rate_limit(
            query_id,
            RateLimitParameters(self.rate_limit_name, referrer, None, concurrent_limit),
        )
        decision_explanation = {
            "reason": explanation,
            "policy": self.rate_limit_name,
            "referrer": referrer,
        }
        return QuotaAllowance(
            can_run=can_run,
            max_threads=self._get_max_threads(referrer),
            explanation=decision_explanation,
        )

    def _update_quota_balance(
        self,
        tenant_ids: dict[str, str | int],
        query_id: str,
        result_or_error: QueryResultOrError,
    ) -> None:
        referrer = str(tenant_ids.get("referrer", "no_referrer"))
        if referrer in _PASS_THROUGH_REFERRERS:
            return

        rate_limit_params = RateLimitParameters(
            self.rate_limit_name,
            referrer,
            None,
            # limit number does not matter for ending a query so I just picked 22
            22,
        )
        self._end_query(query_id, rate_limit_params, result_or_error)
