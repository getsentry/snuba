from __future__ import annotations

import inspect
from collections import Counter
from functools import cached_property

from snuba.configs.configuration import ResourceIdentifier
from snuba.datasets.storages.storage_key import StorageKey
from snuba.query.allocation_policies import QueryResultOrError
from snuba.query.allocation_policies.keyed_allocation_policy import KeyedAllocationPolicy
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
        return f"org:{self._tenant(tenant_ids, 'organization_id')}"


def _policy(**kwargs: object) -> _CountingPolicy:
    return _CountingPerOrg(storage_key=ResourceIdentifier(StorageKey("errors")), **kwargs)


def test_records_against_key() -> None:
    policy = _policy()
    tenant_ids: dict[str, str | int] = {"organization_id": 1, "referrer": "r"}
    assert policy.update_quota_balance(tenant_ids, QUERY_ID, _RESULT) == 1
    assert dict(policy.counts) == {"org:1": 1}


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


def test_missing_tenant_id_rejects() -> None:
    policy = _policy()
    allowance = policy.get_quota_allowance({"referrer": "r"}, QUERY_ID)
    assert not allowance.can_run
    assert allowance.explanation["__name__"] == "InvalidTenantsForAllocationPolicy"
    assert not policy.counts


def test_base_without_key_cannot_be_built() -> None:
    assert inspect.isabstract(_CountingPolicy)
