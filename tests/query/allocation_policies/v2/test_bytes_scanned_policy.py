from __future__ import annotations

import inspect
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
from snuba.query.allocation_policies.v2.base import KeyedAllocationPolicy
from snuba.query.allocation_policies.v2.bytes_scanned_policy import (
    BytesScannedPerOrgPolicy,
    BytesScannedPerOrgRefPolicy,
    BytesScannedPerRefPolicy,
    BytesScannedPolicy,
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


def _scan(policy: AllocationPolicy, tenant_ids: dict[str, str | int], progress_bytes: int) -> None:
    policy.update_quota_balance(
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


def _timeout(policy: AllocationPolicy, tenant_ids: dict[str, str | int]) -> None:
    error = QueryException()
    error.__cause__ = ClickhouseError("timeout", code=ErrorCodes.TIMEOUT_EXCEEDED)
    policy.update_quota_balance(
        tenant_ids, QUERY_ID, QueryResultOrError(query_result=None, error=error)
    )


def test_defaults() -> None:
    policy = BytesScannedPerOrgRefPolicy(storage_key=ResourceIdentifier(StorageKey("errors")))
    assert policy.enforce == "always"
    assert policy.limit == int(1.28 * 10**12)
    assert policy.max_threads == 10
    assert policy.is_enforced is True
    assert policy.is_pardonable is False


@pytest.mark.parametrize(
    "kwargs",
    [
        pytest.param({"enforce": "sometimes"}, id="unknown enforce"),
        pytest.param({"limit": 0}, id="zero limit"),
        pytest.param({"timeout_penalty": 2}, id="penalty above 1"),
    ],
)
def test_invalid_settings_raise(kwargs: dict[str, Any]) -> None:
    with pytest.raises(ValueError):
        _policy(**kwargs)


@pytest.mark.parametrize("cls", list(_BY_KEY.values()))
def test_registered_by_name(cls: type[BytesScannedPolicy]) -> None:
    assert AllocationPolicy.get_from_name(cls.__name__) is cls


def test_base_without_key_cannot_be_built() -> None:
    assert inspect.isabstract(BytesScannedPolicy)


def test_is_a_keyed_policy() -> None:
    assert issubclass(BytesScannedPolicy, KeyedAllocationPolicy)


@pytest.mark.redis_db
def test_rejects_once_limit_is_scanned() -> None:
    policy = _policy()
    allowance = policy.get_quota_allowance(TENANT_IDS, QUERY_ID)
    assert allowance.can_run
    assert allowance.max_threads == MAX_THREADS

    _scan(policy, TENANT_IDS, LIMIT)
    allowance = policy.get_quota_allowance(TENANT_IDS, QUERY_ID)
    assert not allowance.can_run
    assert allowance.max_threads == 0
    assert allowance.quota_used == LIMIT
    assert allowance.explanation["limit"] == LIMIT


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
    ],
)
@pytest.mark.redis_db
def test_missing_tenant_id_rejects(key_by: str, tenant_ids: dict[str, str | int]) -> None:
    allowance = _policy(key_by=key_by).get_quota_allowance(tenant_ids, QUERY_ID)
    assert not allowance.can_run


@pytest.mark.redis_db
def test_cross_org_query_is_skipped() -> None:
    policy = _policy()
    tenant_ids: dict[str, str | int] = {"cross_org_query": 1, "referrer": "some_referrer"}
    _scan(policy, tenant_ids, LIMIT * 100)
    assert policy.get_quota_allowance(tenant_ids, QUERY_ID).can_run


@pytest.mark.redis_db
def test_timeout_counts_share_of_limit() -> None:
    policy = _policy(timeout_penalty=0.5)
    _timeout(policy, TENANT_IDS)
    assert policy.get_quota_allowance(TENANT_IDS, QUERY_ID).quota_used == LIMIT // 2
    _timeout(policy, TENANT_IDS)
    assert not policy.get_quota_allowance(TENANT_IDS, QUERY_ID).can_run


@pytest.mark.redis_db
def test_other_errors_count_nothing() -> None:
    policy = _policy()
    error = QueryException()
    error.__cause__ = ClickhouseError("read timeout", code=-1)
    policy.update_quota_balance(
        TENANT_IDS, QUERY_ID, QueryResultOrError(query_result=None, error=error)
    )
    assert policy.get_quota_allowance(TENANT_IDS, QUERY_ID).quota_used == 0


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


def test_resolver_import_registers_v2_policies() -> None:
    """The resolver looks policies up by name, so importing it alone must register
    every v2 policy. Runs in a fresh interpreter: this test module already imports
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
