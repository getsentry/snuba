from __future__ import annotations

import logging

from snuba.query.allocation_policies import (
    NO_SUGGESTION,
    AllocationPolicyConfig,
    QueryResultOrError,
    QuotaAllowance,
)
from snuba.query.allocation_policies.concurrent_rate_limit import (
    BaseConcurrentRateLimitAllocationPolicy,
)
from snuba.redis import RedisClientKey, get_redis_client
from snuba.state.rate_limit import RateLimitParameters
from snuba.utils.serializable_exception import JsonSerializable

rds = get_redis_client(RedisClientKey.RATE_LIMITER)

logger = logging.getLogger("snuba.query.allocation_policy_per_referrer")

_DEFAULT_MAX_THREADS = 10
_DEFAULT_CONCURRENT_REQUEST_PER_REFERRER = 100
_REFERRER_CONCURRENT_OVERRIDE = -1
_REFERRER_MAX_THREADS_OVERRIDE = -1
_REQUESTS_THROTTLE_DIVIDER = 1.5
_THREADS_THROTTLE_DIVIDER = 2

QUOTA_UNIT = "concurrent_queries"
SUGGESTION = "This feature is doing too many concurrent queries. Customers are being affected arbitrarily. Either means the feature is not being appropriately rate limited on the sentry side or that the queries are inefficient"


class ReferrerGuardRailPolicy(BaseConcurrentRateLimitAllocationPolicy):
    """
    A policy to prevent runaway referrers from consuming too many queries.

    This concern is orthogonal to customer rate limits in its purpose. This rate limiter being tripped is a problem
    caused by sentry developers, not customer abuse. It either means that a feature was release that queries this referrer
    too much or that an appropriate rate limit was not set somewhere upstream. It affects customers randomly and basically
    acts as a load shedder. As a referrer approaches the rate limiter's threshold for rejecting queries, that referrer's
    queries will get throttled. The threshold for throttling and the (reduced) number of threads are configurable via
    _REQUESTS_THROTTLE_DIVIDER and _THREADS_THROTTLE_DIVIDER

    For example, a product team may push out a feature that sends 20 snuba queries every 5 seconds on the UI.
    In that case, that feature should break but others should continue to be served.
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
                default=_DEFAULT_CONCURRENT_REQUEST_PER_REFERRER,
            ),
            AllocationPolicyConfig(
                name="referrer_concurrent_override",
                description="""override the concurrent limit for a referrer""",
                value_type=int,
                param_types={"referrer": str},
                default=_REFERRER_CONCURRENT_OVERRIDE,
            ),
            AllocationPolicyConfig(
                name="referrer_max_threads_override",
                description="""override the max_threads for a referrer, applies to every query made by that referrer""",
                param_types={"referrer": str},
                value_type=int,
                default=_REFERRER_MAX_THREADS_OVERRIDE,
            ),
            AllocationPolicyConfig(
                name="requests_throttle_divider",
                description="default_concurrent_request_per_referrer divided by this value will be the threshold at which we will decrease the number of threads (THROTTLED_THREADS) used to execute queries",
                value_type=float,
                default=_REQUESTS_THROTTLE_DIVIDER,
            ),
            AllocationPolicyConfig(
                name="threads_throttle_divider",
                description="max threads divided by this number is the number of threads we use to execute queries for a throttled referrer",
                value_type=int,
                default=_THREADS_THROTTLE_DIVIDER,
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
        concurrent_limit = self._get_concurrent_limit(referrer)
        rate_limit_params = RateLimitParameters(
            self.rate_limit_name, referrer, None, concurrent_limit
        )
        rate_limit_stats, can_run, explanation = self._is_within_rate_limit(
            query_id,
            rate_limit_params,
        )
        assert (
            rate_limit_params.concurrent_limit is not None
        ), "concurrent_limit must be set"
        num_threads = self._get_max_threads(referrer)
        requests_throttle_threshold = max(
            1,
            int(
                self.get_config_value("default_concurrent_request_per_referrer")
                // self.get_config_value("requests_throttle_divider")
            ),
        )

        is_throttled = False
        if rate_limit_stats.concurrent > requests_throttle_threshold:
            num_threads = max(
                1, num_threads // self.get_config_value("threads_throttle_divider")
            )
            self.metrics.increment(
                "concurrent_queries_throttled", tags={"referrer": referrer}
            )
            is_throttled = True

        self.metrics.timing(
            "concurrent_queries_referrer",
            rate_limit_stats.concurrent,
            tags={"referrer": referrer},
        )
        decision_explanation: dict[str, JsonSerializable] = {
            "reason": explanation,
            "policy": self.rate_limit_name,
            "referrer": referrer,
        }

        return QuotaAllowance(
            can_run=can_run,
            max_threads=num_threads if can_run else 0,
            explanation=decision_explanation,
            is_throttled=is_throttled,
            throttle_threshold=requests_throttle_threshold,
            rejection_threshold=rate_limit_params.concurrent_limit,
            quota_used=rate_limit_stats.concurrent,
            quota_unit=QUOTA_UNIT,
            suggestion=NO_SUGGESTION if can_run else SUGGESTION,
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
