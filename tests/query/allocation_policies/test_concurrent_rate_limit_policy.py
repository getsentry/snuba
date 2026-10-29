from __future__ import annotations

import time
from unittest import mock

import pytest

from snuba.configs.configuration import ResourceIdentifier
from snuba.datasets.storage import StorageKey
from snuba.query.allocation_policies import (
    AllocationPolicyViolations,
    InvalidTenantsForAllocationPolicy,
    QueryResultOrError,
)
from snuba.query.allocation_policies.concurrent_rate_limit import (
    ConcurrentRateLimitAllocationPolicy,
)
from snuba.web import QueryException, QueryResult
from tests.configs.component_config import set_component_config

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

MAX_CONCURRENT_QUERIES = 5
MAX_QUERIES_PER_SECOND = 10


@pytest.fixture(scope="function")
def policy() -> ConcurrentRateLimitAllocationPolicy:
    policy = ConcurrentRateLimitAllocationPolicy(
        storage_key=StorageKey("test"),
        concurrent_limit=MAX_CONCURRENT_QUERIES,
    )
    return policy


@pytest.mark.redis_db
def test_rate_limit_concurrent(policy: ConcurrentRateLimitAllocationPolicy) -> None:
    for i in range(MAX_CONCURRENT_QUERIES):
        policy.get_quota_allowance(tenant_ids={"organization_id": 123}, query_id=f"abc{i}")

    quota_allowance = policy.get_quota_allowance(
        tenant_ids={"organization_id": 123}, query_id=f"abc{MAX_CONCURRENT_QUERIES}"
    )
    assert not quota_allowance.can_run and quota_allowance.max_threads == 0


@pytest.mark.redis_db
def test_rate_limit_concurrent_diff_tenants(
    policy: ConcurrentRateLimitAllocationPolicy,
) -> None:
    RATE_LIMITED_ORG_ID = 123
    OTHER_ORG_ID = 456
    for i in range(MAX_CONCURRENT_QUERIES):
        policy.get_quota_allowance(
            tenant_ids={"organization_id": RATE_LIMITED_ORG_ID}, query_id=f"abc{i}"
        )

    quota_allowance = policy.get_quota_allowance(
        tenant_ids={"organization_id": RATE_LIMITED_ORG_ID},
        query_id=f"abc{MAX_CONCURRENT_QUERIES}",
    )
    assert not quota_allowance.can_run and quota_allowance.max_threads == 0
    policy.get_quota_allowance(
        tenant_ids={"organization_id": OTHER_ORG_ID},
        query_id=f"abc{MAX_CONCURRENT_QUERIES}",
    )


@pytest.mark.redis_db
def test_configure_max_query_duration(
    policy: ConcurrentRateLimitAllocationPolicy,
) -> None:
    max_query_duration_s = 1
    sleep_time = 1.01
    set_component_config(policy, "concurrent_limit", 1)
    set_component_config(policy, "max_query_duration_s", max_query_duration_s)

    policy.get_quota_allowance(tenant_ids={"organization_id": 123}, query_id="abc1")
    time.sleep(sleep_time)
    assert policy.get_quota_allowance(
        tenant_ids={"organization_id": 123}, query_id="abc2"
    ).can_run, (
        "max_query_duration_s is set to {max_query_duration_s}, test sleeps for {sleep_time} seconds, the first query should have no longer been counted towards the concurrent limit"
    )


@pytest.mark.redis_db
def test_rate_limit_concurrent_complete_query(
    policy: ConcurrentRateLimitAllocationPolicy,
) -> None:
    # submit the max concurrent queries
    for i in range(MAX_CONCURRENT_QUERIES):
        policy.get_quota_allowance(tenant_ids={"organization_id": 123}, query_id=f"abc{i}")

    # cant submit anymore
    quota_allowance = policy.get_quota_allowance(
        tenant_ids={"organization_id": 123}, query_id=f"abc{MAX_CONCURRENT_QUERIES}"
    )
    assert not quota_allowance.can_run and quota_allowance.max_threads == 0

    # one query finishes
    policy.update_quota_balance(
        tenant_ids={"organization_id": 123},
        query_id="abc0",
        result_or_error=_RESULT_SUCCESS,
    )

    # can submit another query
    policy.get_quota_allowance(
        tenant_ids={"organization_id": 123}, query_id=f"abc{MAX_CONCURRENT_QUERIES}"
    )

    # but no more than that
    quota_allowance = policy.get_quota_allowance(
        tenant_ids={"organization_id": 123},
        query_id="some_query_id",
    )
    assert not quota_allowance.can_run and quota_allowance.max_threads == 0


@pytest.mark.redis_db
def test_update_quota_balance(policy: ConcurrentRateLimitAllocationPolicy) -> None:
    # test that it doesn't matter if we had an error state or a success state
    # when a query is finished (in whatever state), it is no longer counted as a concurrent query

    for i in range(MAX_CONCURRENT_QUERIES):
        policy.get_quota_allowance(tenant_ids={"organization_id": 123}, query_id=f"abc{i}")

    for i in range(MAX_CONCURRENT_QUERIES):
        policy.update_quota_balance(
            tenant_ids={"organization_id": 123},
            query_id=f"abc{i}",
            result_or_error=_RESULT_FAIL,
        )

    for i in range(MAX_CONCURRENT_QUERIES):
        assert policy.get_quota_allowance(
            tenant_ids={"organization_id": 123}, query_id=f"abc{i}"
        ).can_run


def test_tenant_selection(policy: ConcurrentRateLimitAllocationPolicy):
    tenant_ids: dict[str, int | str] = {"organization_id": 123, "project_id": 456}
    assert policy._get_tenant_key_and_value(tenant_ids) == ("project_id", 456)
    assert policy._get_tenant_key_and_value({"organization_id": 123}) == (
        "organization_id",
        123,
    )
    with pytest.raises(InvalidTenantsForAllocationPolicy):
        policy._get_tenant_key_and_value({})


@pytest.mark.parametrize(
    ("key_by", "tenant_ids", "bucket"),
    [
        pytest.param("tenant", {"organization_id": 1, "project_id": 2}, "2", id="default, project"),
        pytest.param("tenant", {"organization_id": 1}, "1", id="default, org"),
        pytest.param(
            "organization",
            {"organization_id": 1, "project_id": 2, "referrer": "r"},
            "organization_id__1",
            id="organization",
        ),
        pytest.param(
            "organization_referrer",
            {"organization_id": 1, "project_id": 2, "referrer": "r"},
            "organization_id__1|referrer__r",
            id="organization_referrer",
        ),
        pytest.param("organization", {"project_id": 2}, "2", id="no org falls back"),
        pytest.param("typo", {"organization_id": 1, "project_id": 2}, "2", id="unknown value"),
    ],
)
def test_key_by_bucket(key_by: str, tenant_ids: dict[str, str | int], bucket: str) -> None:
    policy = ConcurrentRateLimitAllocationPolicy(
        ResourceIdentifier(StorageKey("errors")), key_by=key_by
    )
    # The default must keep today's bucket, or every running count resets.
    assert policy._get_bucket(tenant_ids) == bucket


@pytest.mark.redis_db
def test_key_by_organization_shares_one_count() -> None:
    policy = ConcurrentRateLimitAllocationPolicy(
        ResourceIdentifier(StorageKey("errors")), key_by="organization", concurrent_limit=1
    )
    assert policy.get_quota_allowance({"organization_id": 1, "project_id": 2}, "q1").can_run
    assert not policy.get_quota_allowance({"organization_id": 1, "project_id": 3}, "q2").can_run
    assert policy.get_quota_allowance({"organization_id": 9, "project_id": 3}, "q3").can_run


@pytest.mark.redis_db
def test_cross_org(policy: ConcurrentRateLimitAllocationPolicy) -> None:
    tenant_ids: dict[str, str | int] = {
        "referrer": "do_something",
        "cross_org_query": 1,
    }
    assert policy.get_quota_allowance(tenant_ids=tenant_ids, query_id="a").can_run
    assert (
        policy.get_quota_allowance(tenant_ids=tenant_ids, query_id="b").max_threads
        == policy.max_threads
    )
    # make sure that this can be called with cross org queries
    # and nothing raises
    policy.update_quota_balance(tenant_ids, "c", None)  # type: ignore[arg-type]


@pytest.mark.redis_db
def test_bad_tenants(policy: ConcurrentRateLimitAllocationPolicy):
    bad_tenant_ids: dict[str, str | int] = {"referrer": "abcd"}
    with mock.patch("snuba.settings.RAISE_ON_ALLOCATION_POLICY_FAILURES", False):
        quota_allowance = policy.get_quota_allowance(bad_tenant_ids, "1234")
        assert not quota_allowance.can_run and quota_allowance.max_threads == 0

        # does not raise
        policy.update_quota_balance(bad_tenant_ids, "1234", _RESULT_SUCCESS)
