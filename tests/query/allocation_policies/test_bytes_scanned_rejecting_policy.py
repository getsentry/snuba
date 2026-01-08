from __future__ import annotations

import pytest
from clickhouse_driver import errors

from snuba.clickhouse.errors import ClickhouseError
from snuba.datasets.storages.storage_key import StorageKey
from snuba.query.allocation_policies import AllocationPolicy, QueryResultOrError
from snuba.query.allocation_policies.bytes_scanned_rejecting_policy import (
    BytesScannedRejectingPolicy,
)
from snuba.web import QueryException, QueryResult

PROJECT_REFERRER_SCAN_LIMIT = 1000
ORGANIZATION_REFERRER_SCAN_LIMIT = PROJECT_REFERRER_SCAN_LIMIT * 2
MAX_THREAD_NUMBER = 400
# This policy does not use the query_id for any of its operation,
# but we need to pass it for the interface
QUERY_ID = "deadbeef"


@pytest.fixture(scope="function")
def policy() -> AllocationPolicy:
    policy = BytesScannedRejectingPolicy(
        storage_key=StorageKey("errors"),
        required_tenant_types=["referrer", "organization_id", "project_id"],
        default_config_overrides={},
    )
    return policy


def _configure_policy(policy: AllocationPolicy) -> None:
    policy.set_config_value("is_active", 1)
    policy.set_config_value("is_enforced", 1)
    policy.set_config_value("max_threads", MAX_THREAD_NUMBER)
    policy.set_config_value("project_referrer_scan_limit", PROJECT_REFERRER_SCAN_LIMIT)
    policy.set_config_value("organization_referrer_scan_limit", ORGANIZATION_REFERRER_SCAN_LIMIT)


@pytest.mark.parametrize(
    ("tenant_ids", "bytes_to_scan", "reason", "limit"),
    [
        pytest.param(
            {
                "organization_id": 123,
                "project_id": 12345,
                "referrer": "some_referrer",
            },
            PROJECT_REFERRER_SCAN_LIMIT,
            f"project_id 12345 is over the bytes scanned limit of {PROJECT_REFERRER_SCAN_LIMIT} for referrer some_referrer",
            PROJECT_REFERRER_SCAN_LIMIT,
            id="choose project_id over organization_id",
        ),
        pytest.param(
            {
                "organization_id": 123,
                "referrer": "some_referrer",
            },
            ORGANIZATION_REFERRER_SCAN_LIMIT,
            f"organization_id 123 is over the bytes scanned limit of {ORGANIZATION_REFERRER_SCAN_LIMIT} for referrer some_referrer",
            ORGANIZATION_REFERRER_SCAN_LIMIT,
            id="choose organization_id when no project id provided",
        ),
    ],
)
@pytest.mark.redis_db
def test_consume_quota(
    policy: BytesScannedRejectingPolicy,
    tenant_ids: dict,
    bytes_to_scan: int,
    reason: str,
    limit: int,
) -> None:
    _configure_policy(policy)
    allowance = policy.get_quota_allowance(tenant_ids, QUERY_ID)
    assert allowance.can_run
    assert allowance.max_threads == MAX_THREAD_NUMBER
    policy.update_quota_balance(
        tenant_ids,
        QUERY_ID,
        QueryResultOrError(
            query_result=QueryResult(
                result={"profile": {"progress_bytes": bytes_to_scan}},
                extra={"stats": {}, "sql": "", "experiments": {}},
            ),
            error=None,
        ),
    )
    allowance = policy.get_quota_allowance(tenant_ids, QUERY_ID)
    assert not allowance.can_run and allowance.max_threads == 0
    assert {
        "granted_quota": 0,
        "limit": limit,
        "storage_key": "errors",
    }.items() <= allowance.explanation.items()

    assert reason in str(allowance.explanation["reason"])
    new_tenant_ids = {**tenant_ids, "referrer": tenant_ids["referrer"] + "abcd"}

    # a different referrer should work fine though
    allowance = policy.get_quota_allowance(
        new_tenant_ids,
        QUERY_ID,
    )
    assert allowance.can_run


@pytest.mark.redis_db
def test_cross_org_query(policy: BytesScannedRejectingPolicy) -> None:
    _configure_policy(policy)
    tenant_ids: dict[str, int | str] = {
        "cross_org_query": 1,
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
                result={"profile": {"progress_bytes": ORGANIZATION_REFERRER_SCAN_LIMIT * 100}},
                extra={"stats": {}, "sql": "", "experiments": {}},
            ),
            error=None,
        ),
    )
    # doesn't apply to cross org queries, should still have no effect
    allowance = policy.get_quota_allowance(tenant_ids, QUERY_ID)
    assert allowance.can_run
    assert allowance.max_threads == MAX_THREAD_NUMBER


@pytest.mark.parametrize(
    ("tenant_ids",),
    [
        pytest.param({"referrer": "no_referrer"}, id="no org or project"),
        pytest.param({"project_id": 123}, id="no referrer"),
    ],
)
def test_invalid_tenants(
    policy: BytesScannedRejectingPolicy, tenant_ids: dict[str, str | int]
) -> None:
    allowance = policy.get_quota_allowance(tenant_ids, QUERY_ID)
    assert not allowance.can_run and allowance.max_threads == 0
    assert allowance.explanation["__name__"] == "InvalidTenantsForAllocationPolicy"


@pytest.mark.parametrize(
    ("tenant_ids", "overrides"),
    [
        pytest.param(
            {
                "organization_id": 123,
                "project_id": 12345,
                "referrer": "some_referrer",
            },
            (
                "referrer_all_projects_scan_limit_override",
                10,
                {"referrer": "some_referrer"},
            ),
            id="use overridden scan limit",
        ),
        pytest.param(
            {
                "organization_id": 123,
                "referrer": "some_referrer",
            },
            (
                "referrer_all_organizations_scan_limit_override",
                100,
                {"referrer": "some_referrer"},
            ),
            id="use overridden scan limit",
        ),
    ],
)
@pytest.mark.redis_db
def test_overrides(
    policy: BytesScannedRejectingPolicy,
    tenant_ids: dict[str, str | int],
    overrides: tuple[str, int, dict[str, str | int]],
) -> None:
    _configure_policy(policy)
    limit = overrides[1]
    policy.set_config_value(*overrides)
    allowance = policy.get_quota_allowance(tenant_ids, QUERY_ID)
    assert allowance.can_run
    policy.update_quota_balance(
        tenant_ids,
        QUERY_ID,
        QueryResultOrError(
            query_result=QueryResult(
                result={"profile": {"progress_bytes": limit}},
                extra={"stats": {}, "sql": "", "experiments": {}},
            ),
            error=None,
        ),
    )
    allowance = policy.get_quota_allowance(tenant_ids, QUERY_ID)
    assert not allowance.can_run and allowance.max_threads == 0
    assert allowance.explanation["limit"] == limit


@pytest.mark.redis_db
def test_penalize_timeout(policy: BytesScannedRejectingPolicy) -> None:
    _configure_policy(policy)
    tenant_ids: dict[str, int | str] = {
        "organization_id": 123,
        "project_id": 12345,
        "referrer": "some_referrer",
    }
    policy.set_config_value(
        "clickhouse_timeout_bytes_scanned_penalization",
        ORGANIZATION_REFERRER_SCAN_LIMIT * 2,
    )
    allowance = policy.get_quota_allowance(tenant_ids, QUERY_ID)

    assert allowance.can_run
    # regular query exception is thrown, should not affect quota
    policy.update_quota_balance(tenant_ids, QUERY_ID, QueryResultOrError(None, QueryException()))
    allowance = policy.get_quota_allowance(tenant_ids, QUERY_ID)
    assert allowance.can_run

    # timneout exception is thrown, the penalization is greater than the quota, therefore
    # next query should be rejected
    timeout_exception = QueryException()
    timeout_exception.__cause__ = ClickhouseError(code=errors.ErrorCodes.TIMEOUT_EXCEEDED)
    policy.update_quota_balance(tenant_ids, QUERY_ID, QueryResultOrError(None, timeout_exception))

    allowance = policy.get_quota_allowance(tenant_ids, QUERY_ID)
    assert not allowance.can_run and allowance.max_threads == 0


@pytest.mark.redis_db
def test_does_not_throttle_and_then_throttles(
    policy: BytesScannedRejectingPolicy,
) -> None:
    tenant_ids = {
        "project_id": 4505240668733440,
        "referrer": "api.trace-explorer.stats",
    }
    bytes_to_scan = 100000001
    policy.set_config_value("bytes_throttle_divider", 100)
    policy.set_config_value(
        "referrer_all_projects_scan_limit_override",
        20000000000,
        {"referrer": "api.trace-explorer.stats"},
    )

    _configure_policy(policy)
    policy.update_quota_balance(
        tenant_ids,
        QUERY_ID,
        QueryResultOrError(
            query_result=QueryResult(
                result={"profile": {"progress_bytes": bytes_to_scan}},
                extra={"stats": {}, "sql": "", "experiments": {}},
            ),
            error=None,
        ),
    )

    allowance = policy.get_quota_allowance(tenant_ids, QUERY_ID)
    assert allowance.is_throttled == False
    assert allowance.max_threads == MAX_THREAD_NUMBER

    policy.update_quota_balance(
        tenant_ids,
        QUERY_ID,
        QueryResultOrError(
            query_result=QueryResult(
                result={"profile": {"progress_bytes": bytes_to_scan}},
                extra={"stats": {}, "sql": "", "experiments": {}},
            ),
            error=None,
        ),
    )
    allowance = policy.get_quota_allowance(tenant_ids, QUERY_ID)
    assert allowance.is_throttled == True
    assert allowance.max_threads == MAX_THREAD_NUMBER // 2


@pytest.mark.redis_db
def test_limit_bytes_read(
    policy: BytesScannedRejectingPolicy,
) -> None:
    tenant_ids = {
        "project_id": 4505240668733440,
        "referrer": "api.trace-explorer.stats",
    }
    scan_limit = 20000000000
    threads_throttle_divider = 2
    max_bytes_to_read_scan_limit_divider = 100
    policy.set_config_value("threads_throttle_divider", threads_throttle_divider)
    policy.set_config_value("bytes_throttle_divider", 100)
    policy.set_config_value("limit_bytes_instead_of_rejecting", 1)
    policy.set_config_value(
        "max_bytes_to_read_scan_limit_divider", max_bytes_to_read_scan_limit_divider
    )

    policy.set_config_value(
        "referrer_all_projects_scan_limit_override",
        scan_limit,
        {"referrer": "api.trace-explorer.stats"},
    )

    _configure_policy(policy)
    policy.update_quota_balance(
        tenant_ids,
        QUERY_ID,
        QueryResultOrError(
            query_result=QueryResult(
                result={"profile": {"progress_bytes": scan_limit}},
                extra={"stats": {}, "sql": "", "experiments": {}},
            ),
            error=None,
        ),
    )

    allowance = policy.get_quota_allowance(tenant_ids, QUERY_ID)
    assert allowance.is_throttled
    assert allowance.max_threads == MAX_THREAD_NUMBER // threads_throttle_divider
    assert allowance.max_bytes_to_read == int(scan_limit / max_bytes_to_read_scan_limit_divider)
