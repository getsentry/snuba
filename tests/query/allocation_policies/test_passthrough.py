from __future__ import annotations

import pytest

from snuba.configs.configuration import ResourceIdentifier
from snuba.query.allocation_policies import AllocationPolicy, QueryResultOrError
from snuba.query.allocation_policies.resolver import get_active_allocation_policies
from snuba.web import QueryResult
from tests.query.allocation_policies.attachment import match_block, override_allocation_policy

LIMIT = 1
# Same org as NORMAL, so the per-org counter would fill up if passthrough counted.
PASSTHROUGH: dict[str, str | int] = {"organization_id": 1, "referrer": "alerts"}
NORMAL: dict[str, str | int] = {"organization_id": 1, "referrer": "dashboard"}
_SCANNED_LIMIT = QueryResultOrError(
    query_result=QueryResult(
        result={"profile": {"progress_bytes": LIMIT}},
        extra={"stats": {}, "sql": "", "experiments": {}},
    ),
    error=None,
)


def _run(
    policy: AllocationPolicy, tenant_ids: dict[str, str | int], query_id: str, finish: bool
) -> bool:
    allowance = policy.get_quota_allowance(tenant_ids, query_id)
    if finish:
        policy.update_quota_balance(tenant_ids, query_id, _SCANNED_LIMIT)
    return allowance.can_run


# Bytes count once a query finishes; a concurrent slot counts until it finishes.
@pytest.mark.redis_db
@pytest.mark.parametrize(
    "name, finish",
    [("BytesScannedPerOrgPolicy", True), ("ConcurrentQueriesPerOrgPolicy", False)],
)
def test_passthrough_runs_uncounted(name: str, finish: bool) -> None:
    block = match_block([{"name": name, "limit": LIMIT, "passthrough": {"referrer": ["alerts"]}}])
    with override_allocation_policy({"EAP": block}):
        (policy,) = get_active_allocation_policies(ResourceIdentifier("EAP"))

    for i in range(3):
        allowance = policy.get_quota_allowance(PASSTHROUGH, f"alert{i}")
        assert allowance.can_run
        assert allowance.explanation["reason"] == "passthrough"
        if finish:
            policy.update_quota_balance(PASSTHROUGH, f"alert{i}", _SCANNED_LIMIT)

    assert _run(policy, NORMAL, "first", finish)
    assert not _run(policy, NORMAL, "second", finish)
