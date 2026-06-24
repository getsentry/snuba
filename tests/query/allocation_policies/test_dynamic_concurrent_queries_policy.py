from __future__ import annotations

import pytest

from snuba.configs.configuration import ResourceIdentifier
from snuba.datasets.storages.storage_key import StorageKey
from snuba.query.allocation_policies import (
    AllocationPolicyViolations,
    QueryResultOrError,
)
from snuba.query.allocation_policies.dynamic_concurrent_queries import (
    DynamicConcurrentQueries,
)
from snuba.web import QueryException, QueryResult

_RESULT_SUCCESS = QueryResultOrError(
    QueryResult(
        result={"profile": {"bytes": 42069}},
        extra={"stats": {}, "sql": "", "experiments": {}},
    ),
    error=None,
)

_QUERY_EXCEPTION = QueryException()
_QUERY_EXCEPTION.__cause__ = AllocationPolicyViolations("some policy was violated")
_RESULT_FAIL = QueryResultOrError(None, error=_QUERY_EXCEPTION)

GLOBAL_SOFT_LIMIT = 10
PER_ORG_SOFT_LIMIT = 4


@pytest.fixture(scope="function")
def policy() -> DynamicConcurrentQueries:
    return DynamicConcurrentQueries(
        storage_key=ResourceIdentifier(StorageKey("test")),
        required_tenant_types=["organization_id"],
        default_config_overrides={
            "global_soft_limit": GLOBAL_SOFT_LIMIT,
            "per_org_soft_limit": PER_ORG_SOFT_LIMIT,
        },
    )


@pytest.mark.redis_db
def test_allows_all_orgs_under_global_soft_limit(
    policy: DynamicConcurrentQueries,
) -> None:
    # Spread queries across orgs while staying under the global soft limit. Even though a
    # single org can go well above its per-org soft limit, everything is allowed because
    # the cluster has spare capacity.
    for i in range(GLOBAL_SOFT_LIMIT):
        allowance = policy.get_quota_allowance(
            tenant_ids={"organization_id": 123}, query_id=f"q{i}"
        )
        assert allowance.can_run, f"query {i} should have been allowed"


@pytest.mark.redis_db
def test_sheds_heavy_org_over_global_soft_limit(
    policy: DynamicConcurrentQueries,
) -> None:
    # One heavy org pushes the cluster past the global soft limit on its own.
    for i in range(GLOBAL_SOFT_LIMIT):
        assert policy.get_quota_allowance(
            tenant_ids={"organization_id": 123}, query_id=f"heavy{i}"
        ).can_run

    # The cluster is now at the global soft limit; the next query crosses it. The heavy
    # org is far above its (proportionally shrunk) effective limit, so it is rejected.
    allowance = policy.get_quota_allowance(
        tenant_ids={"organization_id": 123}, query_id="heavy_over"
    )
    assert not allowance.can_run and allowance.max_threads == 0


@pytest.mark.redis_db
def test_protects_small_org_when_cluster_busy(
    policy: DynamicConcurrentQueries,
) -> None:
    # A heavy org saturates the cluster past the global soft limit.
    for i in range(GLOBAL_SOFT_LIMIT + 5):
        policy.get_quota_allowance(tenant_ids={"organization_id": 123}, query_id=f"heavy{i}")

    # A different org sending only a single query is still allowed even though the cluster
    # is over its global soft limit, because an org may always run at least one query.
    allowance = policy.get_quota_allowance(tenant_ids={"organization_id": 456}, query_id="small1")
    assert allowance.can_run


@pytest.mark.redis_db
def test_finished_queries_free_capacity(
    policy: DynamicConcurrentQueries,
) -> None:
    for i in range(GLOBAL_SOFT_LIMIT):
        policy.get_quota_allowance(tenant_ids={"organization_id": 123}, query_id=f"q{i}")

    assert not policy.get_quota_allowance(
        tenant_ids={"organization_id": 123}, query_id="over"
    ).can_run

    # Finish several queries to drop the global concurrent count back under the soft limit.
    for i in range(GLOBAL_SOFT_LIMIT):
        policy.update_quota_balance(
            tenant_ids={"organization_id": 123},
            query_id=f"q{i}",
            result_or_error=_RESULT_SUCCESS,
        )

    assert policy.get_quota_allowance(tenant_ids={"organization_id": 123}, query_id="after").can_run


@pytest.mark.redis_db
def test_organization_soft_limit_override(
    policy: DynamicConcurrentQueries,
) -> None:
    # Give org 123 a tiny soft limit and a low global soft limit so it gets shed quickly,
    # while org 456 keeps the default per-org soft limit.
    policy.set_config_value("global_soft_limit", 4)
    policy.set_config_value("organization_soft_limit_override", 1, params={"organization_id": 123})

    # Fill the cluster past the global soft limit using org 456.
    for i in range(5):
        policy.get_quota_allowance(tenant_ids={"organization_id": 456}, query_id=f"other{i}")

    # org 123 already has one query running (its overridden soft limit), so a second one
    # is rejected once the cluster is over the global soft limit.
    assert policy.get_quota_allowance(
        tenant_ids={"organization_id": 123}, query_id="o123_1"
    ).can_run
    assert not policy.get_quota_allowance(
        tenant_ids={"organization_id": 123}, query_id="o123_2"
    ).can_run


@pytest.mark.redis_db
def test_rejected_query_not_counted_after_balance_update(
    policy: DynamicConcurrentQueries,
) -> None:
    for i in range(GLOBAL_SOFT_LIMIT):
        policy.get_quota_allowance(tenant_ids={"organization_id": 123}, query_id=f"q{i}")
    rejected = policy.get_quota_allowance(tenant_ids={"organization_id": 123}, query_id="rejected")
    assert not rejected.can_run

    # The rejected query is removed from both buckets so it does not leak concurrency.
    policy.update_quota_balance(
        tenant_ids={"organization_id": 123},
        query_id="rejected",
        result_or_error=_RESULT_FAIL,
    )


def test_pass_through_referrer(policy: DynamicConcurrentQueries) -> None:
    for i in range(GLOBAL_SOFT_LIMIT * 2):
        assert policy.get_quota_allowance(
            tenant_ids={"referrer": "subscriptions_executor", "organization_id": 1},
            query_id=f"q{i}",
        ).can_run


@pytest.mark.redis_db
def test_cross_org(policy: DynamicConcurrentQueries) -> None:
    tenant_ids: dict[str, str | int] = {
        "referrer": "do_something",
        "cross_org_query": 1,
    }
    assert policy.get_quota_allowance(tenant_ids=tenant_ids, query_id="a").can_run
    # update must not raise for cross-org queries
    policy.update_quota_balance(tenant_ids, "a", _RESULT_SUCCESS)


@pytest.mark.redis_db
def test_missing_organization_id_rejected(
    policy: DynamicConcurrentQueries,
) -> None:
    from unittest import mock

    with mock.patch("snuba.settings.RAISE_ON_ALLOCATION_POLICY_FAILURES", False):
        allowance = policy.get_quota_allowance({"referrer": "abcd"}, "1234")
        assert not allowance.can_run and allowance.max_threads == 0
        # does not raise
        policy.update_quota_balance({"referrer": "abcd"}, "1234", _RESULT_SUCCESS)
