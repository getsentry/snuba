from __future__ import annotations

from typing import Any

import pytest
from clickhouse_driver import errors

from snuba.clickhouse.errors import ClickhouseError
from snuba.configs.configuration import ResourceIdentifier
from snuba.datasets.storages.storage_key import StorageKey
from snuba.query.allocation_policies import AllocationPolicy, QueryResultOrError
from snuba.query.allocation_policies.bytes_scanned_rejecting_policy import (
    BytesScannedRejectingPolicy,
)
from snuba.web import QueryException, QueryResult
from tests.configs.component_config import (
    ComponentConfigOverride,
    override_component_configs,
)

PROJECT_REFERRER_SCAN_LIMIT = 1000
ORGANIZATION_REFERRER_SCAN_LIMIT = PROJECT_REFERRER_SCAN_LIMIT * 2
MAX_THREAD_NUMBER = 400
# This policy does not use the query_id for any of its operation,
# but we need to pass it for the interface
QUERY_ID = "deadbeef"


@pytest.fixture(scope="function")
def policy() -> AllocationPolicy:
    policy = BytesScannedRejectingPolicy(
        storage_key=ResourceIdentifier(StorageKey("errors")),
        required_tenant_types=["referrer", "organization_id", "project_id"],
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
        (policy, "project_referrer_scan_limit", PROJECT_REFERRER_SCAN_LIMIT),
        (policy, "organization_referrer_scan_limit", ORGANIZATION_REFERRER_SCAN_LIMIT),
    ]


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
    tenant_ids: dict[str, str | int],
    bytes_to_scan: int,
    reason: str,
    limit: int,
) -> None:
    with override_component_configs(*_base_config_overrides(policy)):
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
        new_tenant_ids: dict[str, str | int] = {
            **tenant_ids,
            "referrer": str(tenant_ids["referrer"]) + "abcd",
        }

        # a different referrer should work fine though
        allowance = policy.get_quota_allowance(
            new_tenant_ids,
            QUERY_ID,
        )
        assert allowance.can_run


@pytest.mark.redis_db
def test_cross_org_query(policy: BytesScannedRejectingPolicy) -> None:
    tenant_ids: dict[str, int | str] = {
        "cross_org_query": 1,
        "referrer": "some_referrer",
    }
    with override_component_configs(*_base_config_overrides(policy)):
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
    ("tenant_ids", "override_entry", "limit"),
    [
        pytest.param(
            {
                "organization_id": 123,
                "project_id": 12345,
                "referrer": "some_referrer",
            },
            ("referrer_all_projects_scan_limit_override", 10, {"referrer": "some_referrer"}),
            10,
            id="project referrer override",
        ),
        pytest.param(
            {
                "organization_id": 123,
                "referrer": "some_referrer",
            },
            ("organization_referrer_scan_limit_overrides", {"*": {"some_referrer": 100}}),
            100,
            id="all-organizations referrer override",
        ),
        pytest.param(
            {
                "organization_id": 123,
                "referrer": "some_referrer",
            },
            ("organization_referrer_scan_limit_overrides", {"123": {"some_referrer": 50}}),
            50,
            id="per (org, referrer) override",
        ),
        pytest.param(
            {
                "organization_id": 123,
                "referrer": "some_referrer",
            },
            ("organization_referrer_scan_limit_overrides", {"123": {"*": 75}}),
            75,
            id="per-org override",
        ),
    ],
)
@pytest.mark.redis_db
def test_overrides(
    policy: BytesScannedRejectingPolicy,
    tenant_ids: dict[str, str | int],
    override_entry: tuple[str, Any] | tuple[str, Any, dict[str, str | int]],
    limit: int,
) -> None:
    with override_component_configs(
        *_base_config_overrides(policy),
        (policy, *override_entry),
    ):
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
def test_org_override_precedence(policy: BytesScannedRejectingPolicy) -> None:
    """(org_id, referrer) > org_id > (all orgs, referrer) > default."""
    tenant_ids: dict[str, str | int] = {
        "organization_id": 123,
        "referrer": "some_referrer",
    }
    with override_component_configs(
        *_base_config_overrides(policy),
        (
            policy,
            "organization_referrer_scan_limit_overrides",
            {
                "123": {"some_referrer": 100, "*": 500},
                "*": {"some_referrer": 1000},
            },
        ),
    ):
        allowance = policy.get_quota_allowance(tenant_ids, QUERY_ID)
        assert allowance.can_run
        assert allowance.rejection_threshold == 100

        policy.update_quota_balance(
            tenant_ids,
            QUERY_ID,
            QueryResultOrError(
                query_result=QueryResult(
                    result={"profile": {"progress_bytes": 100}},
                    extra={"stats": {}, "sql": "", "experiments": {}},
                ),
                error=None,
            ),
        )
        allowance = policy.get_quota_allowance(tenant_ids, QUERY_ID)
        assert not allowance.can_run
        assert allowance.explanation["limit"] == 100

        # different org still uses the all-orgs referrer override
        other_org_tenant: dict[str, str | int] = {
            "organization_id": 456,
            "referrer": "some_referrer",
        }
        allowance = policy.get_quota_allowance(other_org_tenant, QUERY_ID)
        assert allowance.rejection_threshold == 1000


@pytest.mark.redis_db
def test_org_max_bytes_to_read_cap(policy: BytesScannedRejectingPolicy) -> None:
    """Per-org cap sets max_bytes_to_read and bypasses the sliding-window check."""
    tenant_ids: dict[str, str | int] = {
        "organization_id": 123,
        "referrer": "some_referrer",
    }
    with override_component_configs(
        *_base_config_overrides(policy),
        (policy, "organization_max_bytes_to_read", 500, {"organization_id": 123}),
    ):
        # Exhaust the quota first; the cap should still let the query through.
        policy.update_quota_balance(
            tenant_ids,
            QUERY_ID,
            QueryResultOrError(
                query_result=QueryResult(
                    result={"profile": {"progress_bytes": ORGANIZATION_REFERRER_SCAN_LIMIT * 10}},
                    extra={"stats": {}, "sql": "", "experiments": {}},
                ),
                error=None,
            ),
        )

        allowance = policy.get_quota_allowance(tenant_ids, QUERY_ID)
        assert allowance.can_run
        assert allowance.max_bytes_to_read == 500
        assert allowance.max_threads == MAX_THREAD_NUMBER
        assert not allowance.is_throttled

        # An org without a cap configured is still subject to the sliding-window limit.
        other_org_tenant: dict[str, str | int] = {
            "organization_id": 456,
            "referrer": "some_referrer",
        }
        policy.update_quota_balance(
            other_org_tenant,
            QUERY_ID,
            QueryResultOrError(
                query_result=QueryResult(
                    result={"profile": {"progress_bytes": ORGANIZATION_REFERRER_SCAN_LIMIT}},
                    extra={"stats": {}, "sql": "", "experiments": {}},
                ),
                error=None,
            ),
        )
        allowance = policy.get_quota_allowance(other_org_tenant, QUERY_ID)
        assert not allowance.can_run


@pytest.mark.redis_db
def test_org_cap_does_not_record_into_sliding_window(
    policy: BytesScannedRejectingPolicy,
) -> None:
    """Capped org queries must not accumulate usage in the sliding window.

    `_get_quota_allowance` bypasses the window when an org cap is set;
    `_update_quota_balance` must match, otherwise removing the cap later
    would reject queries against a window that silently filled up while
    the cap was active.
    """
    tenant_ids: dict[str, str | int] = {
        "organization_id": 123,
        "referrer": "some_referrer",
    }
    with override_component_configs(
        *_base_config_overrides(policy),
        (policy, "organization_max_bytes_to_read", 500, {"organization_id": 123}),
    ):
        # While the cap is set, lots of bytes scanned must not be recorded.
        for _ in range(5):
            policy.update_quota_balance(
                tenant_ids,
                QUERY_ID,
                QueryResultOrError(
                    query_result=QueryResult(
                        result={"profile": {"progress_bytes": ORGANIZATION_REFERRER_SCAN_LIMIT}},
                        extra={"stats": {}, "sql": "", "experiments": {}},
                    ),
                    error=None,
                ),
            )

    # Remove the cap; the sliding window should be empty, so the next query
    # is allowed under the normal org-referrer limit.
    with override_component_configs(
        *_base_config_overrides(policy),
        (policy, "organization_max_bytes_to_read", -1, {"organization_id": 123}),
    ):
        allowance = policy.get_quota_allowance(tenant_ids, QUERY_ID)
        assert allowance.can_run
        assert allowance.max_threads == MAX_THREAD_NUMBER
        assert not allowance.is_throttled


@pytest.mark.redis_db
def test_org_referrer_cap_beats_org_cap(policy: BytesScannedRejectingPolicy) -> None:
    """(org_id, referrer) cap is more specific than the org_id cap and wins."""
    tenant_ids: dict[str, str | int] = {
        "organization_id": 123,
        "referrer": "some_referrer",
    }
    with override_component_configs(
        *_base_config_overrides(policy),
        (policy, "organization_max_bytes_to_read", 1000, {"organization_id": 123}),
        (
            policy,
            "organization_referrer_max_bytes_to_read",
            200,
            {"organization_id": 123, "referrer": "some_referrer"},
        ),
    ):
        allowance = policy.get_quota_allowance(tenant_ids, QUERY_ID)
        assert allowance.can_run
        assert allowance.max_bytes_to_read == 200

        # A different referrer falls back to the org-wide cap.
        other_referrer: dict[str, str | int] = {
            "organization_id": 123,
            "referrer": "other_referrer",
        }
        allowance = policy.get_quota_allowance(other_referrer, QUERY_ID)
        assert allowance.can_run
        assert allowance.max_bytes_to_read == 1000


@pytest.mark.redis_db
def test_org_caps_do_not_apply_to_project_queries(
    policy: BytesScannedRejectingPolicy,
) -> None:
    """An org-level cap must not bypass project-level sliding-window limits.

    Sentry queries usually carry both organization_id and project_id; the
    policy resolves those to the project_id branch. The org cap should only
    fire on org-keyed queries.
    """
    tenant_ids: dict[str, str | int] = {
        "organization_id": 123,
        "project_id": 12345,
        "referrer": "some_referrer",
    }
    with override_component_configs(
        *_base_config_overrides(policy),
        (policy, "organization_max_bytes_to_read", 500, {"organization_id": 123}),
        (
            policy,
            "organization_referrer_max_bytes_to_read",
            200,
            {"organization_id": 123, "referrer": "some_referrer"},
        ),
    ):
        # Exhaust the project quota; the org cap must not let the query through.
        policy.update_quota_balance(
            tenant_ids,
            QUERY_ID,
            QueryResultOrError(
                query_result=QueryResult(
                    result={"profile": {"progress_bytes": PROJECT_REFERRER_SCAN_LIMIT}},
                    extra={"stats": {}, "sql": "", "experiments": {}},
                ),
                error=None,
            ),
        )
        allowance = policy.get_quota_allowance(tenant_ids, QUERY_ID)
        assert not allowance.can_run
        assert allowance.max_bytes_to_read == 0


@pytest.mark.redis_db
def test_penalize_timeout(policy: BytesScannedRejectingPolicy) -> None:
    tenant_ids: dict[str, int | str] = {
        "organization_id": 123,
        "project_id": 12345,
        "referrer": "some_referrer",
    }
    with override_component_configs(
        *_base_config_overrides(policy),
        (
            policy,
            "clickhouse_timeout_bytes_scanned_penalization",
            ORGANIZATION_REFERRER_SCAN_LIMIT * 2,
        ),
    ):
        allowance = policy.get_quota_allowance(tenant_ids, QUERY_ID)

        assert allowance.can_run
        # regular query exception is thrown, should not affect quota
        policy.update_quota_balance(
            tenant_ids, QUERY_ID, QueryResultOrError(None, QueryException())
        )
        allowance = policy.get_quota_allowance(tenant_ids, QUERY_ID)
        assert allowance.can_run

        # timneout exception is thrown, the penalization is greater than the quota, therefore
        # next query should be rejected
        timeout_exception = QueryException()
        timeout_exception.__cause__ = ClickhouseError(code=errors.ErrorCodes.TIMEOUT_EXCEEDED)
        policy.update_quota_balance(
            tenant_ids, QUERY_ID, QueryResultOrError(None, timeout_exception)
        )

        allowance = policy.get_quota_allowance(tenant_ids, QUERY_ID)
        assert not allowance.can_run and allowance.max_threads == 0


@pytest.mark.redis_db
def test_does_not_throttle_and_then_throttles(
    policy: BytesScannedRejectingPolicy,
) -> None:
    tenant_ids: dict[str, str | int] = {
        "project_id": 4505240668733440,
        "referrer": "api.trace-explorer.stats",
    }
    bytes_to_scan = 100000001
    with override_component_configs(
        *_base_config_overrides(policy),
        (policy, "bytes_throttle_divider", 100),
        (
            policy,
            "referrer_all_projects_scan_limit_override",
            20000000000,
            {"referrer": "api.trace-explorer.stats"},
        ),
    ):
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
        assert not allowance.is_throttled
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
        assert allowance.is_throttled
        assert allowance.max_threads == MAX_THREAD_NUMBER // 2


@pytest.mark.redis_db
def test_limit_bytes_read(
    policy: BytesScannedRejectingPolicy,
) -> None:
    tenant_ids: dict[str, str | int] = {
        "project_id": 4505240668733440,
        "referrer": "api.trace-explorer.stats",
    }
    scan_limit = 20000000000
    threads_throttle_divider = 2
    max_bytes_to_read_scan_limit_divider = 100
    with override_component_configs(
        *_base_config_overrides(policy),
        (policy, "threads_throttle_divider", threads_throttle_divider),
        (policy, "bytes_throttle_divider", 100),
        (policy, "limit_bytes_instead_of_rejecting", 1),
        (policy, "max_bytes_to_read_scan_limit_divider", max_bytes_to_read_scan_limit_divider),
        (
            policy,
            "referrer_all_projects_scan_limit_override",
            scan_limit,
            {"referrer": "api.trace-explorer.stats"},
        ),
    ):
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
