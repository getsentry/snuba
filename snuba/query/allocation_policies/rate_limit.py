from __future__ import annotations

from snuba.query.allocation_policies import (
    AllocationPolicy,
    AllocationPolicyConfig,
    QuotaAllowance,
)

DEFAULT_CONCURRENT_QUERIES_LIMIT = 22
DEFAULT_PER_SECOND_QUERIES_LIMIT = 50


class RateLimitAllocationPolicy(AllocationPolicy):
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
                name="per_second_limit",
                description="maximum amount of concurrent queries per tenant",
                value_type=int,
                default=DEFAULT_PER_SECOND_QUERIES_LIMIT,
            ),
            AllocationPolicyConfig(
                name="limit_name",
                description="the name of the rate limit bucket e.g. project_concurrent, don't change this unless you have a really good reason",
                value_type=str,
                default="default",
            ),
            AllocationPolicyConfig(
                name="rate_history_sec",
                description="the amount of seconds timestamps are kept in redis",
                value_type=int,
                default=3600,
            ),
            AllocationPolicyConfig(
                name="rate_limit_shard_factor",
                description="""number of shards that each redis set is supposed to have.
                 increasing this value multiplies the number of redis keys by that
                 factor, and (on average) reduces the size of each redis set""",
                value_type=int,
                default=1,
            ),
        ]

        def _get_quota_allowance(
            self, tenant_ids: dict[str, str | int], query_id: str
        ) -> QuotaAllowance:
            # before a query is run on clickhouse, make a decision whether it can be run and with
            # how many threads
            pass

        def _update_quota_balance(
            self,
            tenant_ids: dict[str, str | int],
            query_id: str,
            result_or_error: QueryResultOrError,
        ) -> None:
            # after the query has been run, update whatever this allocation policy
            # keeps track of which will affect subsequent queries
            pass
