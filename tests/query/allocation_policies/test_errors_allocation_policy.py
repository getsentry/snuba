from __future__ import annotations

import pytest

from snuba.datasets.storages.storage_key import StorageKey
from snuba.query.allocation_policies import (
    AllocationPolicy,
    AllocationPolicyViolation,
    InvalidPolicyConfig,
    QueryResultOrError,
)
from snuba.query.allocation_policies.errors_allocation_policy import (
    _ORG_LESS_REFERRERS,
    ErrorsAllocationPolicy,
)
from snuba.state import set_config
from snuba.web import QueryResult

ORG_SCAN_LIMIT = 1000
THROTTLED_THREAD_NUMBER = 1
MAX_THREAD_NUMBER = 400


@pytest.fixture(scope="function")
def policy() -> ErrorsAllocationPolicy:
    policy = ErrorsAllocationPolicy(
        storage_key=StorageKey("errors"),
        required_tenant_types=["referrer", "organization_id"],
    )
    return policy


def _configure_policy(policy: AllocationPolicy) -> None:
    policy.set_config("is_active", 1)
    policy.set_config("is_enforced", 1)
    policy.set_config("org_limit_bytes_scanned", ORG_SCAN_LIMIT)
    policy.set_config("throttled_thread_number", THROTTLED_THREAD_NUMBER)
    set_config("query_settings/max_threads", MAX_THREAD_NUMBER)


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
    assert allowance.max_threads == MAX_THREAD_NUMBER
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
    assert allowance.max_threads == THROTTLED_THREAD_NUMBER
    assert allowance.explanation == {
        "reason": f"organization 123 is over the bytes scanned limit of {ORG_SCAN_LIMIT}",
        "is_enforced": True,
        "granted_quota": 0,
        "limit": ORG_SCAN_LIMIT,
    }


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
    assert allowance.max_threads == MAX_THREAD_NUMBER


@pytest.mark.redis_db
def test_killswitch(policy: AllocationPolicy) -> None:
    _configure_policy(policy)
    policy.set_config("is_active", 0)
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
    assert allowance.max_threads == MAX_THREAD_NUMBER


@pytest.mark.redis_db
def test_enforcement_switch(policy: AllocationPolicy) -> None:
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
    policy.set_config("is_enforced", 0)
    allowance = policy.get_quota_allowance(tenant_ids)
    # policy not enforced
    assert allowance.max_threads == MAX_THREAD_NUMBER


@pytest.mark.redis_db
def test_reject_queries_without_tenant_ids(policy: AllocationPolicy) -> None:
    _configure_policy(policy)
    with pytest.raises(AllocationPolicyViolation):
        policy.get_quota_allowance(tenant_ids={"organization_id": 1234})
    with pytest.raises(AllocationPolicyViolation):
        policy.get_quota_allowance(tenant_ids={"referrer": "bloop"})
    # These should not fail because we know they don't have an org id
    for referrer in _ORG_LESS_REFERRERS:
        tenant_ids = {"referrer": referrer}
        policy.get_quota_allowance(tenant_ids)
        policy.update_quota_balance(
            tenant_ids,
            QueryResultOrError(
                query_result=QueryResult(
                    result={"profile": {"bytes": ORG_SCAN_LIMIT}}, extra={}  # type: ignore
                ),
                error=None,
            ),
        )


@pytest.mark.redis_db
def test_bad_config_keys(policy: AllocationPolicy) -> None:
    _configure_policy(policy)
    with pytest.raises(InvalidPolicyConfig) as err:
        policy.set_config("bad_config", 1)
    assert (
        str(err.value)
        == "'bad_config' is not a valid config for ErrorsAllocationPolicy!"
    )
    with pytest.raises(InvalidPolicyConfig) as err:
        policy.set_config("throttled_thread_number", "bad_value")
    assert str(err.value) == "'bad_value' (str) is not of expected type: int"


@pytest.mark.redis_db
def test_config_values(policy: AllocationPolicy) -> None:
    _configure_policy(policy)
    assert set(policy.configurable_params().keys()) == set(
        [
            "is_enforced",
            "is_active",
            "org_limit_bytes_scanned",
            "throttled_thread_number",
        ]
    )
    assert policy.config_values() == {
        "is_active": 1,
        "is_enforced": 1,
        "org_limit_bytes_scanned": ORG_SCAN_LIMIT,
        "throttled_thread_number": THROTTLED_THREAD_NUMBER,
    }
    policy.set_config("org_limit_bytes_scanned", 100)
    assert policy.config_values() == {
        "is_active": 1,
        "is_enforced": 1,
        "org_limit_bytes_scanned": 100,
        "throttled_thread_number": THROTTLED_THREAD_NUMBER,
    }


@pytest.mark.redis_db
def test_passthrough_subscriptions(policy) -> None:
    _configure_policy(policy)
    # currently subscriptions are not throttled due to them being on the critical path
    # this test makes sure that no matter how much quota they consume, they are not throttled
    tenant_ids = {"referrer": "subscriptions_executor", "organization_id": 1}
    assert (
        policy.get_quota_allowance(tenant_ids=tenant_ids).max_threads
        == MAX_THREAD_NUMBER
    )
    policy.update_quota_balance(
        tenant_ids,
        QueryResultOrError(
            query_result=QueryResult(
                result={"profile": {"bytes": ORG_SCAN_LIMIT * 1000}}, extra={}
            ),
            error=None,
        ),
    )
    assert (
        policy.get_quota_allowance(tenant_ids=tenant_ids).max_threads
        == MAX_THREAD_NUMBER
    )
