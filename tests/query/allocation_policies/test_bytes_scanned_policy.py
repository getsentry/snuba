from __future__ import annotations

import os
import subprocess
import sys
from typing import Any

import pytest

from snuba.clickhouse.error_codes import ErrorCodes
from snuba.clickhouse.errors import ClickhouseError
from snuba.clusters.load_info import LoadInfo
from snuba.configs.configuration import ResourceIdentifier
from snuba.datasets.storages.storage_key import StorageKey
from snuba.query.allocation_policies import (
    AllocationPolicy,
    QueryResultOrError,
    QuotaAllowanceDecision,
)
from snuba.query.allocation_policies.bytes_scanned_policy import (
    TERABYTE,
    BytesScannedPerOrgPolicy,
    BytesScannedPerOrgRefPolicy,
    BytesScannedPerRefPolicy,
    BytesScannedPolicy,
    Enforce,
)
from snuba.web import QueryException, QueryResult

LIMIT = 1000
MAX_THREADS = 16
QUERY_ID = "deadbeef"
TENANT_IDS: dict[str, str | int] = {"organization_id": 1, "referrer": "some_referrer"}
IDLE = LoadInfo(cluster_load=10, concurrent_queries=1)
BUSY = LoadInfo(cluster_load=500, concurrent_queries=100)
THRESHOLDS = {"reject": {"cluster_load": 200, "concurrent_queries": 50}}


_BY_KEY: dict[str, type[BytesScannedPolicy]] = {
    "org": BytesScannedPerOrgPolicy,
    "ref": BytesScannedPerRefPolicy,
    "org_ref": BytesScannedPerOrgRefPolicy,
}


def _policy(key_by: str = "org_ref", **kwargs: Any) -> BytesScannedPolicy:
    return _BY_KEY[key_by](
        storage_key=ResourceIdentifier(StorageKey("errors")),
        **{"limit": LIMIT, "max_threads": MAX_THREADS, **kwargs},
    )


def _scan(
    policy: AllocationPolicy, tenant_ids: dict[str, str | int], progress_bytes: int
) -> int | None:
    return policy.update_quota_balance(
        tenant_ids,
        QUERY_ID,
        QueryResultOrError(
            query_result=QueryResult(
                result={"profile": {"progress_bytes": progress_bytes}},
                extra={"stats": {}, "sql": "", "experiments": {}},
            ),
            error=None,
        ),
    )


def _timeout(policy: AllocationPolicy, tenant_ids: dict[str, str | int]) -> int | None:
    error = QueryException()
    error.__cause__ = ClickhouseError("timeout", code=ErrorCodes.TIMEOUT_EXCEEDED)
    return policy.update_quota_balance(
        tenant_ids, QUERY_ID, QueryResultOrError(query_result=None, error=error)
    )


def test_defaults() -> None:
    policy = BytesScannedPerOrgRefPolicy(storage_key=ResourceIdentifier(StorageKey("errors")))
    assert policy._enforce is Enforce.ALWAYS
    assert policy._limit == int(1.28 * TERABYTE)
    assert policy.max_threads == 10
    assert policy.is_enforced is True
    assert policy.is_pardonable is False
    assert policy._time_window == 600
    assert policy._quota("org:1").window_seconds == 600
    assert policy.additional_config_definitions() == []


def test_unknown_key_raises_type_error() -> None:
    with pytest.raises(TypeError):
        BytesScannedPerOrgPolicy.from_kwargs(storage_key="errors", is_enforced=0)


@pytest.mark.parametrize("cls", list(_BY_KEY.values()))
def test_registered_by_name(cls: type[BytesScannedPolicy]) -> None:
    assert AllocationPolicy.get_from_name(cls.__name__) is cls


def test_base_without_key_raises() -> None:
    policy = BytesScannedPolicy(storage_key=ResourceIdentifier(StorageKey("errors")))
    with pytest.raises(NotImplementedError):
        policy._key(TENANT_IDS)


@pytest.mark.redis_db
def test_rejects_once_limit_is_scanned() -> None:
    policy = _policy()
    allowance = policy.get_quota_allowance(TENANT_IDS, QUERY_ID)
    assert allowance.can_run
    assert allowance.max_threads == MAX_THREADS

    assert _scan(policy, TENANT_IDS, LIMIT) == LIMIT
    allowance = policy.get_quota_allowance(TENANT_IDS, QUERY_ID)
    assert not allowance.can_run
    assert allowance.max_threads == 0
    assert allowance.quota_used == LIMIT
    assert allowance.rejection_threshold == LIMIT


@pytest.mark.parametrize(
    ("key_by", "same_key", "other_key"),
    [
        pytest.param(
            "org_ref",
            {"organization_id": 1, "referrer": "some_referrer", "project_id": 99},
            {"organization_id": 1, "referrer": "other_referrer"},
            id="org_ref ignores project_id, splits by referrer",
        ),
        pytest.param(
            "org",
            {"organization_id": 1, "referrer": "other_referrer"},
            {"organization_id": 2, "referrer": "some_referrer"},
            id="org shares across referrers",
        ),
        pytest.param(
            "ref",
            {"organization_id": 2, "referrer": "some_referrer"},
            {"organization_id": 1, "referrer": "other_referrer"},
            id="ref shares across orgs",
        ),
    ],
)
@pytest.mark.redis_db
def test_key_by(
    key_by: str, same_key: dict[str, str | int], other_key: dict[str, str | int]
) -> None:
    policy = _policy(key_by=key_by)
    _scan(policy, TENANT_IDS, LIMIT)
    assert not policy.get_quota_allowance(same_key, QUERY_ID).can_run
    assert policy.get_quota_allowance(other_key, QUERY_ID).can_run


@pytest.mark.parametrize(
    ("key_by", "key"),
    [
        pytest.param("org", "org:1", id="org"),
        pytest.param("ref", "ref:some_referrer", id="ref"),
        pytest.param("org_ref", "org:1|ref:some_referrer", id="org_ref ignores project_id"),
    ],
)
def test_key(key_by: str, key: str) -> None:
    # These strings name live Redis counters; changing them resets every key's usage.
    tenant_ids: dict[str, str | int] = {
        "organization_id": 1,
        "referrer": "some_referrer",
        "project_id": 99,
    }
    assert _policy(key_by=key_by)._key(tenant_ids) == key


def test_time_window_reaches_quota() -> None:
    policy = _policy(time_window=3600)
    assert policy._quota("org:1").window_seconds == 3600
    assert "3600 seconds" in policy.suggestion


def test_time_window_in_prefix() -> None:
    # Different windows on one key must not share a Redis counter.
    short = _policy(key_by="ref", time_window=600)._quota("ref:some_referrer")
    long = _policy(key_by="ref", time_window=3600)._quota("ref:some_referrer")
    assert short.prefix_override != long.prefix_override
    assert short.prefix_override is not None and "-600s-" in short.prefix_override


@pytest.mark.redis_db
def test_different_windows_count_separately() -> None:
    short = _policy(key_by="ref", time_window=600)
    long = _policy(key_by="ref", time_window=3600)
    _scan(short, TENANT_IDS, LIMIT)
    assert not short.get_quota_allowance(TENANT_IDS, QUERY_ID).can_run
    assert long.get_quota_allowance(TENANT_IDS, QUERY_ID).can_run


@pytest.mark.redis_db
def test_rejection_suggestion_names_window() -> None:
    policy = _policy(time_window=120)
    _scan(policy, TENANT_IDS, LIMIT)
    allowance = policy.get_quota_allowance(TENANT_IDS, QUERY_ID)
    assert not allowance.can_run
    assert "120 seconds" in allowance.suggestion


@pytest.mark.redis_db
def test_per_key_policies_count_separately() -> None:
    org_ref = _policy(key_by="org_ref")
    org = _policy(key_by="org", limit=LIMIT * 10)
    _scan(org_ref, TENANT_IDS, LIMIT)
    assert not org_ref.get_quota_allowance(TENANT_IDS, QUERY_ID).can_run
    assert org.get_quota_allowance(TENANT_IDS, QUERY_ID).can_run


@pytest.mark.parametrize(
    ("key_by", "tenant_ids"),
    [
        pytest.param("org_ref", {"referrer": "some_referrer"}, id="org_ref without org"),
        pytest.param("org", {"referrer": "some_referrer"}, id="org without org"),
        pytest.param("ref", {"organization_id": 1}, id="ref without referrer"),
        pytest.param("org_ref", {"organization_id": 1}, id="org_ref without referrer"),
    ],
)
@pytest.mark.redis_db
def test_missing_tenant_id_rejects(key_by: str, tenant_ids: dict[str, str | int]) -> None:
    allowance = _policy(key_by=key_by).get_quota_allowance(tenant_ids, QUERY_ID)
    assert not allowance.can_run
    assert allowance.explanation["__name__"] == "InvalidTenantsForAllocationPolicy"


@pytest.mark.redis_db
def test_cross_org_query_is_skipped() -> None:
    policy = _policy()
    tenant_ids: dict[str, str | int] = {"cross_org_query": 1, "referrer": "some_referrer"}
    assert _scan(policy, tenant_ids, LIMIT * 100) == 0
    assert policy.get_quota_allowance(tenant_ids, QUERY_ID).can_run


@pytest.mark.redis_db
def test_timeout_counts_share_of_limit() -> None:
    policy = _policy(timeout_penalty=0.5)
    assert _timeout(policy, TENANT_IDS) == LIMIT // 2
    assert policy.get_quota_allowance(TENANT_IDS, QUERY_ID).quota_used == LIMIT // 2
    _timeout(policy, TENANT_IDS)
    assert not policy.get_quota_allowance(TENANT_IDS, QUERY_ID).can_run


@pytest.mark.redis_db
def test_other_errors_count_nothing() -> None:
    policy = _policy()
    error = QueryException()
    error.__cause__ = ClickhouseError("read timeout", code=-1)
    recorded = policy.update_quota_balance(
        TENANT_IDS, QUERY_ID, QueryResultOrError(query_result=None, error=error)
    )
    assert recorded == 0
    assert policy.get_quota_allowance(TENANT_IDS, QUERY_ID).quota_used == 0


@pytest.mark.parametrize(
    "result",
    [
        pytest.param({}, id="no profile"),
        pytest.param({"profile": None}, id="null profile"),
        pytest.param({"profile": {}}, id="no progress_bytes"),
        pytest.param({"profile": {"progress_bytes": None}}, id="null progress_bytes"),
    ],
)
@pytest.mark.redis_db
def test_missing_progress_bytes_counts_nothing(result: dict[str, Any]) -> None:
    policy = _policy()
    recorded = policy.update_quota_balance(
        TENANT_IDS,
        QUERY_ID,
        QueryResultOrError(
            query_result=QueryResult(
                result=result, extra={"stats": {}, "sql": "", "experiments": {}}
            ),
            error=None,
        ),
    )
    assert recorded == 0


@pytest.mark.parametrize(
    ("enforce", "load_info", "can_run", "decision"),
    [
        pytest.param("always", IDLE, False, QuotaAllowanceDecision.REJECTED, id="always idle"),
        pytest.param("always", BUSY, False, QuotaAllowanceDecision.REJECTED, id="always busy"),
        pytest.param("pardon", IDLE, False, QuotaAllowanceDecision.PARDONED, id="pardon idle"),
        pytest.param("pardon", BUSY, False, QuotaAllowanceDecision.REJECTED, id="pardon busy"),
        pytest.param("pardon", None, False, QuotaAllowanceDecision.REJECTED, id="pardon no load"),
        pytest.param("never", BUSY, True, QuotaAllowanceDecision.ALLOWED, id="never busy"),
    ],
)
@pytest.mark.redis_db
def test_enforce_over_limit(
    enforce: str,
    load_info: LoadInfo | None,
    can_run: bool,
    decision: QuotaAllowanceDecision,
) -> None:
    """Callers (db_query, storage routing) re-derive the decision from the
    allowance, so check what they will see, not just can_run."""
    policy = _policy(enforce=enforce, thresholds=THRESHOLDS)
    _scan(policy, TENANT_IDS, LIMIT)
    allowance = policy.get_quota_allowance(TENANT_IDS, QUERY_ID, load_info)
    assert allowance.can_run is can_run
    assert allowance.decision(policy, load_info) is decision


def test_resolver_import_registers_bytes_scanned_policies() -> None:
    """The resolver looks policies up by name, so importing it alone must register
    every BytesScannedPer* policy. Runs in a fresh interpreter: this test module already imports
    the classes, which would hide a missing registration."""
    code = (
        "import snuba.query.allocation_policies.resolver\n"
        "from snuba.query.allocation_policies import AllocationPolicy\n"
        f"for name in {[c.__name__ for c in _BY_KEY.values()]!r}:\n"
        "    AllocationPolicy.get_from_name(name)\n"
    )
    subprocess.run(
        [sys.executable, "-c", code], check=True, env={**os.environ, "PYTHONPATH": os.getcwd()}
    )
