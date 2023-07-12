import pytest

from snuba.datasets.storage import StorageKey
from snuba.query.allocation_policies import QueryResultOrError
from snuba.query.allocation_policies.rate_limit import RateLimitAllocationPolicy
from snuba.web import QueryResult

_RESULT = QueryResult(
    result={"profile": {"bytes": 42069}},
    extra={"stats": {}, "sql": "", "experiments": {}},
)


@pytest.fixture
def policy() -> RateLimitAllocationPolicy:
    return RateLimitAllocationPolicy(
        storage_key=StorageKey("test"),
        required_tenant_types=["organization_id"],
        default_config_overrides={},
    )


@pytest.mark.redis_db
def test_basic(policy: RateLimitAllocationPolicy) -> None:
    for i in range(30):
        allowance = policy.get_quota_allowance(
            tenant_ids={"organization_id": 123}, query_id=f"abc{i}"
        )
        print(allowance)
    for i in range(30):
        policy.update_quota_balance(
            tenant_ids={"organization_id": 123},
            query_id=f"abc{i}",
            result_or_error=QueryResultOrError(query_result=_RESULT, error=None),
        )
