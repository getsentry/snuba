import pytest

from snuba.datasets.storage import StorageKey
from snuba.query.allocation_policies import (
    AllocationPolicyViolation,
    AllocationPolicyViolations,
    QueryResultOrError,
)
from snuba.query.allocation_policies.rate_limit import RateLimitAllocationPolicy
from snuba.web import QueryException, QueryResult

_RESULT_SUCCESS = QueryResultOrError(
    QueryResult(
        result={"profile": {"bytes": 42069}},
        extra={"stats": {}, "sql": "", "experiments": {}},
    ),
    error=None,
)

_QUERY_EXCEPTION = QueryException()
_QUERY_EXCEPTION.__cause__ = AllocationPolicyViolations("some policy was violated")


_RESULT_FAIL = QueryResultOrError(None, error=_QUERY_EXCEPTION)

MAX_CONCURRENT_QUERIES = 5
MAX_QUERIES_PER_SECOND = 10


@pytest.fixture(scope="function")
def policy() -> RateLimitAllocationPolicy:
    policy = RateLimitAllocationPolicy(
        storage_key=StorageKey("test"),
        required_tenant_types=["organization_id"],
        default_config_overrides={
            "concurrent_limit": MAX_CONCURRENT_QUERIES,
        },
    )
    return policy


@pytest.mark.redis_db
def test_rate_limit_concurrent(policy: RateLimitAllocationPolicy) -> None:
    for i in range(MAX_CONCURRENT_QUERIES):
        policy.get_quota_allowance(
            tenant_ids={"organization_id": 123}, query_id=f"abc{i}"
        )

    with pytest.raises(AllocationPolicyViolation):
        policy.get_quota_allowance(
            tenant_ids={"organization_id": 123}, query_id=f"abc{MAX_CONCURRENT_QUERIES}"
        )


@pytest.mark.redis_db
def test_rate_limit_concurrent_diff_tenants(policy: RateLimitAllocationPolicy) -> None:
    RATE_LIMITED_ORG_ID = 123
    OTHER_ORG_ID = 456
    for i in range(MAX_CONCURRENT_QUERIES):
        policy.get_quota_allowance(
            tenant_ids={"organization_id": RATE_LIMITED_ORG_ID}, query_id=f"abc{i}"
        )

    with pytest.raises(AllocationPolicyViolation):
        policy.get_quota_allowance(
            tenant_ids={"organization_id": RATE_LIMITED_ORG_ID},
            query_id=f"abc{MAX_CONCURRENT_QUERIES}",
        )
    policy.get_quota_allowance(
        tenant_ids={"organization_id": OTHER_ORG_ID},
        query_id=f"abc{MAX_CONCURRENT_QUERIES}",
    )


@pytest.mark.redis_db
def test_rate_limit_concurrent_complete_query(
    policy: RateLimitAllocationPolicy,
) -> None:
    # submit the max concurrent queries
    for i in range(MAX_CONCURRENT_QUERIES):
        policy.get_quota_allowance(
            tenant_ids={"organization_id": 123}, query_id=f"abc{i}"
        )

    # cant submit anymore
    with pytest.raises(AllocationPolicyViolation):
        policy.get_quota_allowance(
            tenant_ids={"organization_id": 123}, query_id=f"abc{MAX_CONCURRENT_QUERIES}"
        )

    # one query finishes
    policy.update_quota_balance(
        tenant_ids={"organization_id": 123},
        query_id="abc0",
        result_or_error=_RESULT_SUCCESS,
    )

    # can submit another query
    policy.get_quota_allowance(
        tenant_ids={"organization_id": 123}, query_id=f"abc{MAX_CONCURRENT_QUERIES}"
    )

    # but no more than that
    with pytest.raises(AllocationPolicyViolation):
        policy.get_quota_allowance(
            tenant_ids={"organization_id": 123},
            query_id="some_query_id",
        )


@pytest.mark.redis_db
def test_update_quota_balance(policy: RateLimitAllocationPolicy) -> None:
    # test that it doesn't matter if we had an error state or a success state
    # when a query is finished (in whatever state), it is no longer counted as a concurrent query

    for i in range(MAX_CONCURRENT_QUERIES):
        policy.get_quota_allowance(
            tenant_ids={"organization_id": 123}, query_id=f"abc{i}"
        )

    for i in range(MAX_CONCURRENT_QUERIES):
        policy.update_quota_balance(
            tenant_ids={"organization_id": 123},
            query_id=f"abc{i}",
            result_or_error=_RESULT_FAIL,
        )

    for i in range(MAX_CONCURRENT_QUERIES):
        assert policy.get_quota_allowance(
            tenant_ids={"organization_id": 123}, query_id=f"abc{i}"
        ).can_run
