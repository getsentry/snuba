from __future__ import annotations

from typing import Any

import pytest

from snuba.configs.configuration import ResourceIdentifier
from snuba.datasets.storages.storage_key import StorageKey
from snuba.query.allocation_policies import (
    AllocationPolicyViolations,
    QueryResultOrError,
)
from snuba.query.allocation_policies.concurrent_queries_policy import (
    ConcurrentQueriesPerOrgPolicy,
    ConcurrentQueriesPerOrgRefPolicy,
    ConcurrentQueriesPerRefPolicy,
    ConcurrentQueriesPolicy,
)
from snuba.query.allocation_policies.resolver import get_active_allocation_policies
from snuba.web import QueryException, QueryResult
from tests.query.allocation_policies.attachment import match_block, override_allocation_policy

LIMIT = 3
MAX_THREADS = 16
TENANT_IDS: dict[str, str | int] = {"organization_id": 1, "referrer": "some_referrer"}

_FINISHED = QueryResultOrError(
    query_result=QueryResult(result={}, extra={"stats": {}, "sql": "", "experiments": {}}),
    error=None,
)
_REJECTION = QueryException()
_REJECTION.__cause__ = AllocationPolicyViolations("rejected")
_REJECTED = QueryResultOrError(query_result=None, error=_REJECTION)


def _policy(
    cls: type[ConcurrentQueriesPolicy] = ConcurrentQueriesPerOrgRefPolicy, **kwargs: Any
) -> ConcurrentQueriesPolicy:
    return cls(
        storage_key=ResourceIdentifier(StorageKey("errors")),
        **{"limit": LIMIT, "max_threads": MAX_THREADS, **kwargs},
    )


def _start(
    policy: ConcurrentQueriesPolicy, query_id: str, tenant_ids: dict[str, str | int] = TENANT_IDS
) -> bool:
    return policy.get_quota_allowance(tenant_ids, query_id).can_run


def _fill(policy: ConcurrentQueriesPolicy, tenant_ids: dict[str, str | int] = TENANT_IDS) -> None:
    for i in range(LIMIT):
        assert _start(policy, f"running{i}", tenant_ids)


@pytest.mark.redis_db
def test_under_limit_runs() -> None:
    allowance = _policy().get_quota_allowance(TENANT_IDS, "q")
    assert allowance.can_run
    assert allowance.max_threads == MAX_THREADS
    assert allowance.quota_used == 1


@pytest.mark.redis_db
def test_at_limit_rejects() -> None:
    policy = _policy()
    _fill(policy)
    allowance = policy.get_quota_allowance(TENANT_IDS, "extra")
    assert not allowance.can_run
    assert allowance.max_threads == 0


@pytest.mark.redis_db
def test_finished_query_frees_its_slot() -> None:
    policy = _policy()
    _fill(policy)
    policy.update_quota_balance(TENANT_IDS, "running0", _FINISHED)
    assert _start(policy, "next")


@pytest.mark.redis_db
def test_rejected_query_takes_no_slot() -> None:
    policy = _policy()
    _fill(policy)
    assert not _start(policy, "rejected")
    policy.update_quota_balance(TENANT_IDS, "rejected", _REJECTED)
    policy.update_quota_balance(TENANT_IDS, "running0", _FINISHED)
    # Had the rejected query stayed registered, this would be over the limit.
    assert _start(policy, "next")


@pytest.mark.redis_db
@pytest.mark.parametrize(
    "cls, same_key, other_key",
    [
        (
            ConcurrentQueriesPerOrgPolicy,
            {"organization_id": 1, "referrer": "other"},
            {"organization_id": 2, "referrer": "some_referrer"},
        ),
        (
            ConcurrentQueriesPerRefPolicy,
            {"organization_id": 2, "referrer": "some_referrer"},
            {"organization_id": 1, "referrer": "other"},
        ),
        (
            ConcurrentQueriesPerOrgRefPolicy,
            TENANT_IDS,
            {"organization_id": 1, "referrer": "other"},
        ),
    ],
)
def test_key_variants(
    cls: type[ConcurrentQueriesPolicy],
    same_key: dict[str, str | int],
    other_key: dict[str, str | int],
) -> None:
    policy = _policy(cls)
    _fill(policy)
    assert not _start(policy, "same", same_key)
    assert _start(policy, "other", other_key)


@pytest.mark.redis_db
def test_enforce_never_runs() -> None:
    policy = _policy(enforce="never")
    _fill(policy)
    allowance = policy.get_quota_allowance(TENANT_IDS, "extra")
    assert allowance.can_run
    assert allowance.max_threads == MAX_THREADS


def test_resolver_attaches_from_match_block() -> None:
    block = match_block(
        [{"name": "ConcurrentQueriesPerOrgPolicy", "limit": 5, "enforce": "pardon"}]
    )
    with override_allocation_policy({"EAP": block}):
        (policy,) = get_active_allocation_policies(ResourceIdentifier("EAP"))
    assert isinstance(policy, ConcurrentQueriesPerOrgPolicy)
    assert policy._limit == 5
    assert policy.is_pardonable


def test_resolver_raises_on_unknown_key() -> None:
    # Stray keys fail every query on the resource, so automator must drop v1 keys
    # when it switches a policy to v2.
    block = match_block([{"name": "ConcurrentQueriesPerOrgPolicy", "concurrent_limit": 5}])
    with override_allocation_policy({"EAP": block}), pytest.raises(TypeError):
        get_active_allocation_policies(ResourceIdentifier("EAP"))
