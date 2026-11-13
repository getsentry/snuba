from __future__ import annotations

import inspect
from collections import Counter
from functools import cached_property

import pytest

from snuba.configs.configuration import ResourceIdentifier
from snuba.datasets.storages.storage_key import StorageKey
from snuba.query.allocation_policies import QueryResultOrError
from snuba.query.allocation_policies.keyed_allocation_policy import (
    KeyedAllocationPolicy,
    org_key,
    org_ref_key,
    ref_key,
)
from snuba.web import QueryResult

QUERY_ID = "deadbeef"
_RESULT = QueryResultOrError(
    query_result=QueryResult(result={}, extra={"stats": {}, "sql": "", "experiments": {}}),
    error=None,
)


class _CountingPolicy(KeyedAllocationPolicy):
    """Counts finished queries per key in memory."""

    quota_unit = "queries"
    DEFAULT_LIMIT = 2
    suggestion = "too many queries"

    @cached_property
    def counts(self) -> Counter[str]:
        return Counter()

    def _check(self, key: str, query_id: str) -> int:
        return self.counts[key]

    def _record(self, key: str, query_id: str, result_or_error: QueryResultOrError) -> int:
        self.counts[key] += 1
        return 1


class _CountingPerOrg(_CountingPolicy):
    def _key(self, tenant_ids: dict[str, str | int]) -> str:
        return org_key(tenant_ids, self)


class _CountingPerRef(_CountingPolicy):
    def _key(self, tenant_ids: dict[str, str | int]) -> str:
        return ref_key(tenant_ids, self)


class _CountingPerOrgRef(_CountingPolicy):
    def _key(self, tenant_ids: dict[str, str | int]) -> str:
        return org_ref_key(tenant_ids, self)


_BY_KEY: dict[str, type[_CountingPolicy]] = {
    "org": _CountingPerOrg,
    "ref": _CountingPerRef,
    "org_ref": _CountingPerOrgRef,
}


def _policy(key_by: str = "org_ref", **kwargs: object) -> _CountingPolicy:
    return _BY_KEY[key_by](storage_key=ResourceIdentifier(StorageKey("errors")), **kwargs)


@pytest.mark.parametrize(
    ("key_by", "tenant_ids", "key"),
    [
        pytest.param("org", {"organization_id": 1, "referrer": "r"}, "org:1", id="org"),
        pytest.param("ref", {"organization_id": 1, "referrer": "r"}, "ref:r", id="ref"),
        pytest.param(
            "org_ref",
            {"organization_id": 1, "referrer": "r", "project_id": 5},
            "org:1|ref:r",
            id="org_ref ignores project",
        ),
    ],
)
def test_keys(key_by: str, tenant_ids: dict[str, str | int], key: str) -> None:
    policy = _policy(key_by=key_by)
    assert policy.update_quota_balance(tenant_ids, QUERY_ID, _RESULT) == 1
    assert dict(policy.counts) == {key: 1}


def test_rejects_at_limit_with_subclass_unit() -> None:
    policy = _policy()
    tenant_ids: dict[str, str | int] = {"organization_id": 1, "referrer": "r"}
    for _ in range(2):
        assert policy.get_quota_allowance(tenant_ids, QUERY_ID).can_run
        policy.update_quota_balance(tenant_ids, QUERY_ID, _RESULT)
    allowance = policy.get_quota_allowance(tenant_ids, QUERY_ID)
    assert not allowance.can_run
    assert allowance.quota_unit == "queries"
    assert allowance.quota_used == 2
    assert allowance.rejection_threshold == 2


def test_cross_org_neither_checks_nor_records() -> None:
    policy = _policy(limit=1)
    tenant_ids: dict[str, str | int] = {"cross_org_query": 1}
    assert policy.update_quota_balance(tenant_ids, QUERY_ID, _RESULT) == 0
    assert not policy.counts
    assert policy.get_quota_allowance(tenant_ids, QUERY_ID).can_run


def test_enforce_maps_to_base_flags() -> None:
    assert (_policy(enforce="always").is_enforced, _policy(enforce="always").is_pardonable) == (
        True,
        False,
    )
    assert (_policy(enforce="pardon").is_enforced, _policy(enforce="pardon").is_pardonable) == (
        True,
        True,
    )
    assert _policy(enforce="never").is_enforced is False


def test_ignores_configurable_component_overrides() -> None:
    assert _policy().additional_config_definitions() == []
    assert _policy(max_threads=4).max_threads == 4


@pytest.mark.parametrize(
    ("key_by", "tenant_ids"),
    [
        pytest.param("org", {"referrer": "r"}, id="org without org"),
        pytest.param("ref", {"organization_id": 1}, id="ref without referrer"),
        pytest.param("org_ref", {"organization_id": 1}, id="org_ref without referrer"),
    ],
)
def test_missing_tenant_id_rejects(key_by: str, tenant_ids: dict[str, str | int]) -> None:
    assert not _policy(key_by=key_by).get_quota_allowance(tenant_ids, QUERY_ID).can_run


def test_base_without_key_cannot_be_built() -> None:
    assert inspect.isabstract(_CountingPolicy)
