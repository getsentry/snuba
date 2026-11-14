"""How BytesScannedPerOrgPolicy, BytesScannedPerRefPolicy and
BytesScannedPerOrgRefPolicy behave when all three are configured in the same
match block: each one keeps its own counter, so a query adds its bytes to all
three, but each reads and rejects only on the counter it keys by.
"""

from __future__ import annotations

from typing import Any

import pytest

from snuba.configs.configuration import ResourceIdentifier
from snuba.query.allocation_policies import AllocationPolicy, QueryResultOrError
from snuba.query.allocation_policies.resolver import get_active_allocation_policies
from snuba.web import QueryResult
from tests.query.allocation_policies.attachment import match_block, override_allocation_policy

RESOURCE = "EAP"
QUERY_ID = "deadbeef"
PER_ORG = "BytesScannedPerOrgPolicy"
PER_REF = "BytesScannedPerRefPolicy"
PER_ORG_REF = "BytesScannedPerOrgRefPolicy"

# Limits high enough that the usage test never rejects.
ALL_THREE: list[dict[str, Any]] = [
    {"name": PER_ORG, "limit": 10_000},
    {"name": PER_REF, "limit": 10_000},
    {"name": PER_ORG_REF, "limit": 10_000},
]


def _tenant(org: int, ref: str) -> dict[str, str | int]:
    return {"organization_id": org, "referrer": ref}


def _policies(specs: list[dict[str, Any]]) -> dict[str, AllocationPolicy]:
    with override_allocation_policy({RESOURCE: match_block(specs)}):
        policies = get_active_allocation_policies(ResourceIdentifier(RESOURCE))
    return {p.class_name(): p for p in policies}


def _run_query(
    policies: dict[str, AllocationPolicy], tenant_ids: dict[str, str | int], progress_bytes: int
) -> None:
    """Record a finished query on every policy, as the query pipeline does."""
    result = QueryResultOrError(
        query_result=QueryResult(
            result={"profile": {"progress_bytes": progress_bytes}},
            extra={"stats": {}, "sql": "", "experiments": {}},
        ),
        error=None,
    )
    for policy in policies.values():
        policy.update_quota_balance(tenant_ids, QUERY_ID, result)


def _used(policy: AllocationPolicy, tenant_ids: dict[str, str | int]) -> int:
    return policy.get_quota_allowance(tenant_ids, QUERY_ID).quota_used


def test_one_block_attaches_all_three() -> None:
    assert list(_policies(ALL_THREE)) == [PER_ORG, PER_REF, PER_ORG_REF]


@pytest.mark.redis_db
def test_each_policy_updates_only_its_own_counter() -> None:
    policies = _policies(ALL_THREE)
    _run_query(policies, _tenant(1, "ref_a"), 100)
    _run_query(policies, _tenant(1, "ref_b"), 20)
    _run_query(policies, _tenant(2, "ref_a"), 3)

    per_org = policies[PER_ORG]
    # Org 1's bytes across both referrers; org 2 only its own.
    assert _used(per_org, _tenant(1, "ref_a")) == 120
    assert _used(per_org, _tenant(1, "any_ref")) == 120
    assert _used(per_org, _tenant(2, "ref_a")) == 3
    assert _used(per_org, _tenant(3, "ref_a")) == 0

    per_ref = policies[PER_REF]
    # ref_a's bytes across both orgs; ref_b only org 1's.
    assert _used(per_ref, _tenant(1, "ref_a")) == 103
    assert _used(per_ref, _tenant(99, "ref_a")) == 103
    assert _used(per_ref, _tenant(1, "ref_b")) == 20
    assert _used(per_ref, _tenant(1, "ref_c")) == 0

    per_org_ref = policies[PER_ORG_REF]
    # Only the exact (org, referrer) pair.
    assert _used(per_org_ref, _tenant(1, "ref_a")) == 100
    assert _used(per_org_ref, _tenant(1, "ref_b")) == 20
    assert _used(per_org_ref, _tenant(2, "ref_a")) == 3
    # Org 2 never ran with ref_b, though both appear in other pairs.
    assert _used(per_org_ref, _tenant(2, "ref_b")) == 0


@pytest.mark.redis_db
def test_limit_rejects_only_through_the_policy_that_reached_it() -> None:
    # Only the per-org+ref limit is low enough to be reached.
    policies = _policies(
        [
            {"name": PER_ORG, "limit": 10_000},
            {"name": PER_REF, "limit": 10_000},
            {"name": PER_ORG_REF, "limit": 100},
        ]
    )
    _run_query(policies, _tenant(1, "ref_a"), 100)

    def can_run(tenant_ids: dict[str, str | int]) -> dict[str, bool]:
        return {
            name: policy.get_quota_allowance(tenant_ids, QUERY_ID).can_run
            for name, policy in policies.items()
        }

    assert can_run(_tenant(1, "ref_a")) == {PER_ORG: True, PER_REF: True, PER_ORG_REF: False}
    # Same org with another referrer, and another org with the same referrer, still run.
    assert can_run(_tenant(1, "ref_b")) == {PER_ORG: True, PER_REF: True, PER_ORG_REF: True}
    assert can_run(_tenant(2, "ref_a")) == {PER_ORG: True, PER_REF: True, PER_ORG_REF: True}
