from __future__ import annotations

import pytest

from snuba.clusters.storage_sets import StorageSetKey
from snuba.query.allocation_policies import (
    AllocationPolicyViolation,
    QueryResultOrError,
)
from snuba.query.allocation_policies.errors_allocation_policy import (
    _ORG_LESS_REFERRERS,
    ErrorsAllocationPolicy,
)
from snuba.state import get_config, set_config
from snuba.web import QueryResult

ORG_SCAN_LIMIT = 1000


@pytest.fixture(scope="function")
def policy():
    policy = ErrorsAllocationPolicy(
        storage_set_key=StorageSetKey("errors"),
        required_tenant_types=["referrer", "organization_id"],
    )
    return policy


def _configure_policy(policy):
    set_config(f"{policy.rate_limit_prefix}.is_active", True)
    set_config(f"{policy.rate_limit_prefix}.is_enforced", True)
    set_config(f"{policy.rate_limit_prefix}.org_scan_limit", ORG_SCAN_LIMIT)


@pytest.mark.redis_db
def test_consume_quota(policy: ErrorsAllocationPolicy) -> None:
    # 1. if you scan the limit of bytes, you get throttled to one thread
    _configure_policy(policy)
    tenant_ids: dict[str, int | str] = {
        "organization_id": 123,
        "referrer": "some_referrer",
    }
    allowance = policy.get_quota_allowance(tenant_ids)
    assert allowance.can_run
    assert allowance.max_threads == 8
    policy.update_quota_balance(
        tenant_ids,
        QueryResultOrError(
            query_result=QueryResult(
                result={"profile": {"bytes": ORG_SCAN_LIMIT}}, extra={}
            ),
            error=None,
        ),
    )
    allowance = policy.get_quota_allowance(tenant_ids)
    assert allowance.can_run
    assert allowance.max_threads == 1


@pytest.mark.redis_db
def test_org_isolation(policy) -> None:
    _configure_policy(policy)

    tenant_ids: dict[str, int | str] = {
        "organization_id": 123,
        "referrer": "some_referrer",
    }
    policy.update_quota_balance(
        tenant_ids,
        QueryResultOrError(
            query_result=QueryResult(
                result={"profile": {"bytes": 20 * ORG_SCAN_LIMIT}}, extra={}
            ),
            error=None,
        ),
    )
    different_tenant_ids: dict[str, int | str] = {
        "organization_id": 1235,
        "referrer": "some_referrer",
    }
    allowance = policy.get_quota_allowance(different_tenant_ids)
    assert allowance.max_threads == 8


@pytest.mark.redis_db
def test_killswitch(policy) -> None:
    set_config(f"{policy.rate_limit_prefix}.is_active", False)
    tenant_ids: dict[str, int | str] = {
        "organization_id": 123,
        "referrer": "some_referrer",
    }
    policy.update_quota_balance(
        tenant_ids,
        QueryResultOrError(
            query_result=QueryResult(
                result={"profile": {"bytes": 20 * ORG_SCAN_LIMIT}}, extra={}
            ),
            error=None,
        ),
    )
    allowance = policy.get_quota_allowance(tenant_ids)
    # policy is not active so no change
    assert allowance.max_threads == 8


@pytest.mark.redis_db
def test_enforcement_switch(policy) -> None:
    tenant_ids: dict[str, int | str] = {
        "organization_id": 123,
        "referrer": "some_referrer",
    }
    policy.update_quota_balance(
        tenant_ids,
        QueryResultOrError(
            query_result=QueryResult(
                result={"profile": {"bytes": 20 * ORG_SCAN_LIMIT}}, extra={}
            ),
            error=None,
        ),
    )
    set_config(f"{policy.rate_limit_prefix}.is_enforced", False)
    assert not get_config(f"{policy.rate_limit_prefix}.is_enforced", True)
    allowance = policy.get_quota_allowance(tenant_ids)
    # policy not enforced
    assert allowance.max_threads == 8


@pytest.mark.redis_db
def test_reject_queries_without_tenant_ids(policy) -> None:
    _configure_policy(policy)
    with pytest.raises(AllocationPolicyViolation):
        policy.get_quota_allowance(tenant_ids={"organization_id": 1234})
    with pytest.raises(AllocationPolicyViolation):
        policy.get_quota_allowance(tenant_ids={"referrer": "bloop"})
    # These should not fail because we know they don't have an org id
    for referrer in _ORG_LESS_REFERRERS:
        policy.get_quota_allowance(tenant_ids={"referrer": referrer})
