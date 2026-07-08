from __future__ import annotations

import pytest

from snuba.datasets.storages.storage_key import StorageKey
from snuba.query.allocation_policies import AllocationPolicy, QueryResultOrError
from snuba.query.allocation_policies.bytes_scanned_window_policy import (
    _ORG_LESS_REFERRERS,
    BytesScannedWindowAllocationPolicy,
)
from snuba.web import QueryResult
from tests.configs.component_config import (
    ComponentConfigOverride,
    override_component_configs,
)

ORG_SCAN_LIMIT = 1000
THROTTLED_THREAD_NUMBER = 1
MAX_THREAD_NUMBER = 400
# This policy does not use the query_id for any of its operation,
# but we need to pass it for the interface
QUERY_ID = "deadbeef"


@pytest.fixture(scope="function")
def policy() -> AllocationPolicy:
    policy = BytesScannedWindowAllocationPolicy(
        storage_key=StorageKey("errors"),
        required_tenant_types=["referrer", "organization_id"],
        default_config_overrides={},
    )
    return policy


def _base_config_overrides(policy: AllocationPolicy) -> list[ComponentConfigOverride]:
    """Base config overrides shared by most tests; combine with any test-specific
    overrides in a single ``override_component_configs`` call (the option is one
    dict, so nesting contexts would drop all but the innermost key)."""
    return [
        (policy, "is_active", 1),
        (policy, "is_enforced", 1),
        (policy, "max_threads", MAX_THREAD_NUMBER),
        (policy, "org_limit_bytes_scanned", ORG_SCAN_LIMIT),
        (policy, "throttled_thread_number", THROTTLED_THREAD_NUMBER),
    ]


@pytest.mark.redis_db
def test_consume_quota(policy: BytesScannedWindowAllocationPolicy) -> None:
    # 1. if you scan the limit of bytes, you get throttled to one thread
    with override_component_configs(*_base_config_overrides(policy)):
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
            QueryResultOrError(
                query_result=QueryResult(
                    result={"profile": {"progress_bytes": ORG_SCAN_LIMIT}},
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
            "storage_key": "errors",
        }


@pytest.mark.redis_db
def test_org_isolation(policy: AllocationPolicy) -> None:
    with override_component_configs(*_base_config_overrides(policy)):
        tenant_ids: dict[str, int | str] = {
            "organization_id": 123,
            "referrer": "some_referrer",
        }
        policy.update_quota_balance(
            tenant_ids,
            QUERY_ID,
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
    with override_component_configs(
        *_base_config_overrides(policy),
        (policy, "is_active", 0),
    ):
        tenant_ids: dict[str, int | str] = {
            "organization_id": 123,
            "referrer": "some_referrer",
        }
        policy.update_quota_balance(
            tenant_ids,
            QUERY_ID,
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
    tenant_ids: dict[str, int | str] = {
        "organization_id": 123,
        "referrer": "some_referrer",
    }
    # While enforced, exhausting the quota throttles the query. The sliding-window
    # usage recorded here lives in Redis and persists into the second context.
    with override_component_configs(*_base_config_overrides(policy)):
        policy.update_quota_balance(
            tenant_ids,
            QUERY_ID,
            QueryResultOrError(
                query_result=QueryResult(
                    result={"profile": {"progress_bytes": 20 * ORG_SCAN_LIMIT}},
                    extra={"stats": {}, "sql": "", "experiments": {}},
                ),
                error=None,
            ),
        )
        allowance = policy.get_quota_allowance(tenant_ids, QUERY_ID)
        assert allowance.max_threads == THROTTLED_THREAD_NUMBER

    # Switching enforcement off lets the (still over-quota) query run unthrottled.
    with override_component_configs(
        *_base_config_overrides(policy),
        (policy, "is_enforced", 0),
    ):
        allowance = policy.get_quota_allowance(tenant_ids, QUERY_ID)
        assert allowance.max_threads == MAX_THREAD_NUMBER


@pytest.mark.redis_db
def test_reject_queries_without_tenant_ids(policy: AllocationPolicy) -> None:
    with override_component_configs(*_base_config_overrides(policy)):
        quota_allowance = policy.get_quota_allowance(
            tenant_ids={"organization_id": 1234}, query_id=QUERY_ID
        )
        assert not quota_allowance.can_run and quota_allowance.max_threads == 0

        quota_allowance = policy.get_quota_allowance(
            tenant_ids={"referrer": "bloop"}, query_id=QUERY_ID
        )
        assert not quota_allowance.can_run and quota_allowance.max_threads == 0
        # These should not fail because we know they don't have an org id
        for referrer in _ORG_LESS_REFERRERS:
            tenant_ids: dict[str, str | int] = {"referrer": referrer}
            policy.get_quota_allowance(tenant_ids, QUERY_ID)
            policy.update_quota_balance(
                tenant_ids,
                QUERY_ID,
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
    config_params = policy.config_definitions()
    assert set(config_params.keys()) == {
        "org_limit_bytes_scanned",
        "org_limit_bytes_scanned_override",
        "throttled_thread_number",
        "is_active",
        "is_enforced",
        "max_threads",
    }
    with override_component_configs(*_base_config_overrides(policy)):
        assert policy.get_config_value("org_limit_bytes_scanned") == ORG_SCAN_LIMIT
    with override_component_configs(
        *_base_config_overrides(policy),
        (policy, "org_limit_bytes_scanned", 100),
    ):
        assert policy.get_config_value("org_limit_bytes_scanned") == 100


@pytest.mark.redis_db
def test_passthrough_subscriptions(policy: AllocationPolicy) -> None:
    # currently subscriptions are not throttled due to them being on the critical path
    # this test makes sure that no matter how much quota they consume, they are not throttled
    with override_component_configs(*_base_config_overrides(policy)):
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
    with override_component_configs(*_base_config_overrides(policy)):
        tenant_ids: dict[str, str | int] = {"referrer": "delete-events-from-file"}
        assert policy.get_quota_allowance(tenant_ids=tenant_ids, query_id=QUERY_ID).max_threads == 1
        policy.update_quota_balance(
            tenant_ids,
            QUERY_ID,
            QueryResultOrError(
                query_result=QueryResult(
                    result={"profile": {"bytes": ORG_SCAN_LIMIT * 1000}},
                    extra={"stats": {}, "sql": "", "experiments": {}},
                ),
                error=None,
            ),
        )
        assert policy.get_quota_allowance(tenant_ids=tenant_ids, query_id=QUERY_ID).max_threads == 1


@pytest.mark.redis_db
def test_no_bytes_scanned(policy: AllocationPolicy) -> None:
    with override_component_configs(*_base_config_overrides(policy)):
        tenant_ids: dict[str, str | int] = {
            "referrer": "do_something",
            "organization_id": 1,
        }
        no_bytes_scanned_info_result = QueryResultOrError(
            query_result=QueryResult(
                result={
                    "profile": {
                        # bytes scanned info is missing
                    }
                },
                extra={"stats": {}, "sql": "", "experiments": {}},
            ),
            error=None,
        )
        policy.update_quota_balance(tenant_ids, QUERY_ID, no_bytes_scanned_info_result)


@pytest.mark.redis_db
def test_cross_org(policy: AllocationPolicy) -> None:
    with override_component_configs(*_base_config_overrides(policy)):
        tenant_ids: dict[str, str | int] = {
            "referrer": "do_something",
            "cross_org_query": 1,
        }
        assert policy.get_quota_allowance(tenant_ids=tenant_ids, query_id=QUERY_ID).can_run
        assert (
            policy.get_quota_allowance(tenant_ids=tenant_ids, query_id=QUERY_ID).max_threads
            == policy.max_threads
        )
        # make sure that this can be called with cross org queries
        # and nothing raises
        policy.update_quota_balance(tenant_ids, QUERY_ID, None)  # type: ignore[arg-type]
