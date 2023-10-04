from __future__ import annotations

import time

import pytest

from snuba.datasets.storage import StorageKey
from snuba.query.allocation_policies import (
    AllocationPolicyViolation,
    AllocationPolicyViolations,
    QueryResultOrError,
)
from snuba.query.allocation_policies.concurrent_rate_limit import (
    ConcurrentRateLimitAllocationPolicy,
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

MAX_CONCURRENT_QUERIES = 5
MAX_QUERIES_PER_SECOND = 10


@pytest.fixture(scope="function")
def policy() -> ConcurrentRateLimitAllocationPolicy:
    policy = ConcurrentRateLimitAllocationPolicy(
        storage_key=StorageKey("test"),
        required_tenant_types=["organization_id"],
        default_config_overrides={
            "concurrent_limit": MAX_CONCURRENT_QUERIES,
        },
    )
    return policy


@pytest.mark.redis_db
def test_rate_limit_concurrent(policy: ConcurrentRateLimitAllocationPolicy) -> None:
    for i in range(MAX_CONCURRENT_QUERIES):
        policy.get_quota_allowance(
            tenant_ids={"organization_id": 123}, query_id=f"abc{i}"
        )

    with pytest.raises(AllocationPolicyViolation):
        policy.get_quota_allowance(
            tenant_ids={"organization_id": 123}, query_id=f"abc{MAX_CONCURRENT_QUERIES}"
        )


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

    with pytest.raises(AllocationPolicyViolation):
        policy.get_quota_allowance(
            tenant_ids={"organization_id": RATE_LIMITED_ORG_ID},
            query_id=f"abc{MAX_CONCURRENT_QUERIES}",
        )
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
    policy.set_config_value("concurrent_limit", 1)
    policy.set_config_value("max_query_duration_s", max_query_duration_s)

    policy.get_quota_allowance(tenant_ids={"organization_id": 123}, query_id="abc1")
    time.sleep(sleep_time)
    try:
        policy.get_quota_allowance(tenant_ids={"organization_id": 123}, query_id="abc2")
    except AllocationPolicyViolation:
        assert (
            False
        ), "max_query_duration_s is set to {max_query_duration_s}, test sleeps for {sleep_time} seconds, the first query should have no longer been counted towards the concurrent limit"


@pytest.mark.redis_db
def test_rate_limit_concurrent_complete_query(
    policy: ConcurrentRateLimitAllocationPolicy,
) -> None:
    # submit the max concurrent queries
    for i in range(MAX_CONCURRENT_QUERIES):
        policy.get_quota_allowance(
            tenant_ids={"organization_id": 123}, query_id=f"abc{i}"
        )

    # cant submit anymore
    with pytest.raises(AllocationPolicyViolation):
        policy.get_quota_allowance(
            tenant_ids={"organization_id": 123}, query_id=f"abc{MAX_CONCURRENT_QUERIES}"
        )

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
    with pytest.raises(AllocationPolicyViolation):
        policy.get_quota_allowance(
            tenant_ids={"organization_id": 123},
            query_id="some_query_id",
        )


@pytest.mark.redis_db
def test_update_quota_balance(policy: ConcurrentRateLimitAllocationPolicy) -> None:
    # test that it doesn't matter if we had an error state or a success state
    # when a query is finished (in whatever state), it is no longer counted as a concurrent query

    for i in range(MAX_CONCURRENT_QUERIES):
        policy.get_quota_allowance(
            tenant_ids={"organization_id": 123}, query_id=f"abc{i}"
        )

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
    with pytest.raises(AllocationPolicyViolation):
        policy._get_tenant_key_and_value({})


OVERRIDE_TEST_CASES = [
    pytest.param(
        [("organization_override", 1, {"organization_id": 123})],
        {"organization_id": 123},
        {"organization_id__123": 1},
        1,
        id="organization_override",
    ),
    pytest.param(
        [("organization_override", 1, {"organization_id": 123})],
        {"organization_id": 456},
        {},
        MAX_CONCURRENT_QUERIES,
        id="non-matching tenant_id",
    ),
    pytest.param(
        [
            (
                "referrer_organization_override",
                1,
                {"referrer": "abcd", "organization_id": 456},
            )
        ],
        {"organization_id": 456, "referrer": "abcd"},
        {"organization_id__456|referrer__abcd": 1},
        1,
    ),
    pytest.param(
        [
            ("referrer_project_override", 1, {"referrer": "abcd", "project_id": 134}),
            ("project_override", 4, {"project_id": 134}),
        ],
        {"organization_id": 456, "referrer": "abcd", "project_id": 134},
        {"project_id__134|referrer__abcd": 1, "project_id__134": 4},
        1,
    ),
    pytest.param(
        [
            (
                "referrer_organization_override",
                1,
                {"referrer": "abcd", "organization_id": 123},
            ),
        ],
        {"organization_id": 123, "referrer": "abcd", "project_id": 134},
        {"organization_id__123|referrer__abcd": 1},
        1,
        id="referrer_organization_override",
    ),
    pytest.param(
        [
            (
                "referrer_project_override",
                1,
                {"referrer": "abcd", "project_id": 456},
            ),
        ],
        {"organization_id": 123, "referrer": "abcd", "project_id": 456},
        {"project_id__456|referrer__abcd": 1},
        1,
        id="referrer_organization_override",
    ),
    pytest.param(
        [
            (
                "referrer_project_override",
                MAX_CONCURRENT_QUERIES * 2,
                {"referrer": "abcd", "project_id": 456},
            ),
        ],
        {"organization_id": 123, "referrer": "abcd", "project_id": 456},
        {"project_id__456|referrer__abcd": MAX_CONCURRENT_QUERIES * 2},
        MAX_CONCURRENT_QUERIES * 2,
        id="override to a greater number",
    ),
]


@pytest.mark.redis_db
@pytest.mark.parametrize(
    "overrides,tenant_ids,expected_overrides,expected_concurrent_limit",
    OVERRIDE_TEST_CASES,
)
def test_apply_overrides(
    policy: ConcurrentRateLimitAllocationPolicy,
    overrides,
    tenant_ids,
    expected_overrides,
    expected_concurrent_limit,
) -> None:
    for override in overrides:
        policy.set_config_value(*override)
    for i in range(expected_concurrent_limit):
        policy.get_quota_allowance(tenant_ids=tenant_ids, query_id=f"{i}")
    with pytest.raises(AllocationPolicyViolation) as e:
        policy.get_quota_allowance(
            tenant_ids=tenant_ids, query_id=f"{expected_concurrent_limit+1}"
        )
    assert e.value.explanation["overrides"] == expected_overrides


@pytest.mark.redis_db
def test_override_isolation(
    policy: ConcurrentRateLimitAllocationPolicy,
) -> None:
    override_concurrent_limit = 1
    project_id = 1234
    overridden_referrer = "overridden_referrer"
    policy.set_config_value(
        "referrer_project_override",
        override_concurrent_limit,
        {"project_id": project_id, "referrer": overridden_referrer},
    )
    for i in range(MAX_CONCURRENT_QUERIES):
        policy.get_quota_allowance(
            tenant_ids={"project_id": project_id, "referrer": "a_different_referrer"},
            query_id=str(i),
        )
    # query the override referrer, it should not reject because overrides are counted on their own
    try:
        policy.get_quota_allowance(
            tenant_ids={"project_id": project_id, "referrer": overridden_referrer},
            query_id="uniq_string_1",
        )
    except AllocationPolicyViolation:
        pytest.fail("overridden limits should not be affected by defaults")

    # finish the overridden referrer query, make sure another one can run
    policy.update_quota_balance(
        tenant_ids={"project_id": project_id, "referrer": overridden_referrer},
        query_id="uniq_string_1",
        result_or_error=_RESULT_SUCCESS,
    )
    try:
        policy.get_quota_allowance(
            tenant_ids={"project_id": project_id, "referrer": overridden_referrer},
            query_id="uniq_string_2",
        )
    except Exception:
        pytest.fail(
            "overridden query was finished, another one should have been able to run"
        )

    # finish a non-overidden query
    policy.update_quota_balance(
        tenant_ids={"project_id": project_id, "referrer": "a_different_referrer"},
        query_id="1",
        result_or_error=_RESULT_SUCCESS,
    )

    # our overridden referrer should still error because an update to the non-overridden limits shouldn't affect the overridden ones
    with pytest.raises(AllocationPolicyViolation):
        policy.get_quota_allowance(
            tenant_ids={"project_id": project_id, "referrer": overridden_referrer},
            query_id="uniq_string_3",
        )


def test_pass_through(policy: ConcurrentRateLimitAllocationPolicy) -> None:
    ## should not be blocked because the subscriptions_executor referrer is not rate limited
    try:
        for i in range(MAX_CONCURRENT_QUERIES * 2):
            policy.get_quota_allowance(
                tenant_ids={"referrer": "subscriptions_executor", "project_id": 1234},
                query_id=f"abc{i}",
            )
    except AllocationPolicyViolation:
        pytest.fail("should not have been blocked")
