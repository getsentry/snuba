from __future__ import annotations

import pytest

from snuba.datasets.storages.storage_key import StorageKey
from snuba.query.allocation_policies import (
    AllocationPolicy,
    AllocationPolicyViolation,
    QueryResultOrError,
)
from snuba.query.allocation_policies.bytes_scanned_window_policy import (
    _ORG_LESS_REFERRERS,
    BytesScannedWindowAllocationPolicy,
)
from snuba.web import QueryResult

ORG_SCAN_LIMIT = 1000
THROTTLED_THREAD_NUMBER = 1
MAX_THREAD_NUMBER = 400
# This policy does not use the query_id for any of its operation,
# but we need to pass it for the interface
QUERY_ID = "deadbeef"
DATASET_NAME = "some_dataset"


@pytest.fixture(scope="function")
def policy() -> AllocationPolicy:
    policy = BytesScannedWindowAllocationPolicy(
        storage_key=StorageKey("errors"),
        required_tenant_types=["referrer", "organization_id"],
        default_config_overrides={},
    )
    return policy


def _configure_policy(policy: AllocationPolicy) -> None:
    policy.set_config_value("is_active", 1)
    policy.set_config_value("is_enforced", 1)
    policy.set_config_value("max_threads", MAX_THREAD_NUMBER)
    policy.set_config_value("org_limit_bytes_scanned", ORG_SCAN_LIMIT)
    policy.set_config_value("throttled_thread_number", THROTTLED_THREAD_NUMBER)


@pytest.mark.redis_db
def test_consume_quota(policy: BytesScannedWindowAllocationPolicy) -> None:
    # 1. if you scan the limit of bytes, you get throttled to one thread
    _configure_policy(policy)
    tenant_ids: dict[str, int | str] = {
        "organization_id": 123,
        "referrer": "some_referrer",
    }
    allowance = policy.get_quota_allowance(tenant_ids, QUERY_ID)
    assert allowance.can_run
    assert allowance.max_threads == MAX_THREAD_NUMBER
    policy.update_quota_balance(
        tenant_ids,
        QUERY_ID,
        DATASET_NAME,
        QueryResultOrError(
            query_result=QueryResult(
                result={"profile": {"bytes": ORG_SCAN_LIMIT}},
                extra={"stats": {}, "sql": "", "experiments": {}},
            ),
            error=None,
        ),
    )
    allowance = policy.get_quota_allowance(tenant_ids, QUERY_ID)
    assert allowance.can_run
    assert allowance.max_threads == THROTTLED_THREAD_NUMBER
    assert allowance.explanation == {
        "reason": f"organization 123 is over the bytes scanned limit of {ORG_SCAN_LIMIT}",
        "is_enforced": True,
        "granted_quota": 0,
        "limit": ORG_SCAN_LIMIT,
    }


@pytest.mark.redis_db
def test_org_isolation(policy: AllocationPolicy) -> None:
    _configure_policy(policy)

    tenant_ids: dict[str, int | str] = {
        "organization_id": 123,
        "referrer": "some_referrer",
    }
    policy.update_quota_balance(
        tenant_ids,
        QUERY_ID,
        DATASET_NAME,
        QueryResultOrError(
            query_result=QueryResult(
                result={"profile": {"bytes": 20 * ORG_SCAN_LIMIT}},
                extra={"stats": {}, "sql": "", "experiments": {}},
            ),
            error=None,
        ),
    )
    different_tenant_ids: dict[str, int | str] = {
        "organization_id": 1235,
        "referrer": "some_referrer",
    }
    allowance = policy.get_quota_allowance(different_tenant_ids, QUERY_ID)
    assert allowance.max_threads == MAX_THREAD_NUMBER


@pytest.mark.redis_db
def test_killswitch(policy: AllocationPolicy) -> None:
    _configure_policy(policy)
    policy.set_config_value("is_active", 0)
    tenant_ids: dict[str, int | str] = {
        "organization_id": 123,
        "referrer": "some_referrer",
    }
    policy.update_quota_balance(
        tenant_ids,
        QUERY_ID,
        DATASET_NAME,
        QueryResultOrError(
            query_result=QueryResult(
                result={"profile": {"bytes": 20 * ORG_SCAN_LIMIT}},
                extra={"stats": {}, "sql": "", "experiments": {}},
            ),
            error=None,
        ),
    )
    allowance = policy.get_quota_allowance(tenant_ids, QUERY_ID)
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
        QUERY_ID,
        DATASET_NAME,
        QueryResultOrError(
            query_result=QueryResult(
                result={"profile": {"bytes": 20 * ORG_SCAN_LIMIT}},
                extra={"stats": {}, "sql": "", "experiments": {}},
            ),
            error=None,
        ),
    )
    policy.set_config_value("is_enforced", 0)
    allowance = policy.get_quota_allowance(tenant_ids, QUERY_ID)
    # policy not enforced
    assert allowance.max_threads == MAX_THREAD_NUMBER


@pytest.mark.redis_db
def test_reject_queries_without_tenant_ids(policy: AllocationPolicy) -> None:
    _configure_policy(policy)
    with pytest.raises(AllocationPolicyViolation):
        policy.get_quota_allowance(
            tenant_ids={"organization_id": 1234}, query_id=QUERY_ID
        )
    with pytest.raises(AllocationPolicyViolation):
        policy.get_quota_allowance(tenant_ids={"referrer": "bloop"}, query_id=QUERY_ID)
    # These should not fail because we know they don't have an org id
    for referrer in _ORG_LESS_REFERRERS:
        tenant_ids: dict[str, str | int] = {"referrer": referrer}
        policy.get_quota_allowance(tenant_ids, QUERY_ID)
        policy.update_quota_balance(
            tenant_ids,
            QUERY_ID,
            DATASET_NAME,
            QueryResultOrError(
                query_result=QueryResult(
                    result={"profile": {"bytes": ORG_SCAN_LIMIT}},
                    extra={"stats": {}, "sql": "", "experiments": {}},
                ),
                error=None,
            ),
        )


@pytest.mark.redis_db
def test_simple_config_values(policy: AllocationPolicy) -> None:
    _configure_policy(policy)
    config_params = policy.config_definitions()
    assert set(config_params.keys()) == {
        "org_limit_bytes_scanned",
        "org_limit_bytes_scanned_override",
        "throttled_thread_number",
        "is_active",
        "is_enforced",
        "max_threads",
    }
    assert policy.get_config_value("org_limit_bytes_scanned") == ORG_SCAN_LIMIT
    policy.set_config_value("org_limit_bytes_scanned", 100)
    assert policy.get_config_value("org_limit_bytes_scanned") == 100


@pytest.mark.redis_db
def test_passthrough_subscriptions(policy: AllocationPolicy) -> None:
    _configure_policy(policy)
    # currently subscriptions are not throttled due to them being on the critical path
    # this test makes sure that no matter how much quota they consume, they are not throttled
    tenant_ids: dict[str, str | int] = {
        "referrer": "subscriptions_executor",
        "organization_id": 1,
    }
    assert (
        policy.get_quota_allowance(tenant_ids=tenant_ids, query_id=QUERY_ID).max_threads
        == MAX_THREAD_NUMBER
    )
    policy.update_quota_balance(
        tenant_ids,
        QUERY_ID,
        DATASET_NAME,
        QueryResultOrError(
            query_result=QueryResult(
                result={"profile": {"bytes": ORG_SCAN_LIMIT * 1000}},
                extra={"stats": {}, "sql": "", "experiments": {}},
            ),
            error=None,
        ),
    )
    assert (
        policy.get_quota_allowance(tenant_ids=tenant_ids, query_id=QUERY_ID).max_threads
        == MAX_THREAD_NUMBER
    )


@pytest.mark.redis_db
def test_single_thread_referrers(policy: AllocationPolicy) -> None:
    _configure_policy(policy)
    tenant_ids: dict[str, str | int] = {"referrer": "delete-events-from-file"}
    assert (
        policy.get_quota_allowance(tenant_ids=tenant_ids, query_id=QUERY_ID).max_threads
        == 1
    )
    policy.update_quota_balance(
        tenant_ids,
        QUERY_ID,
        DATASET_NAME,
        QueryResultOrError(
            query_result=QueryResult(
                result={"profile": {"bytes": ORG_SCAN_LIMIT * 1000}},
                extra={"stats": {}, "sql": "", "experiments": {}},
            ),
            error=None,
        ),
    )
    assert (
        policy.get_quota_allowance(tenant_ids=tenant_ids, query_id=QUERY_ID).max_threads
        == 1
    )
