from __future__ import annotations

from unittest import mock

import pytest

from snuba.attribution import get_app_id
from snuba.attribution.attribution_info import AttributionInfo
from snuba.clickhouse.columns import ColumnSet
from snuba.clickhouse.query import Query
from snuba.datasets.storages.factory import get_writable_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.query.allocation_policies import (
    MAX_THRESHOLD,
    NO_SUGGESTION,
    NO_UNITS,
    AllocationPolicy,
    AllocationPolicyConfig,
    AllocationPolicyViolations,
    QueryResultOrError,
    QuotaAllowance,
)
from snuba.query.data_source.simple import Table
from snuba.query.dsl import and_cond, column, equals, literal
from snuba.query.query_settings import HTTPQuerySettings
from snuba.web import QueryException
from snuba.web.delete_query import _execute_query


def get_delete_query() -> Query:
    from_clause = Table(
        "search_issues_local_v2",
        ColumnSet([]),
        storage_key=StorageKey.SEARCH_ISSUES,
        allocation_policies=[],
    )
    group_id = 10
    return Query(
        from_clause=from_clause,
        condition=and_cond(
            equals(column("group_id"), literal(group_id)),
            equals(column("project_id"), literal(3)),
        ),
        on_cluster=None,
        is_delete=True,
    )


def test_delete_query_with_rejecting_allocation_policy() -> None:
    # this test does not need the db or a query because the allocation policy
    # should reject the query before it gets to execution
    storage = get_writable_storage(StorageKey("search_issues"))
    attr_into = AttributionInfo(
        get_app_id("blah"),
        {"project_id": 123, "referrer": "r"},
        "blah",
        None,
        None,
        None,
    )
    update_called = False

    class RejectPolicy(AllocationPolicy):
        def _additional_config_definitions(self) -> list[AllocationPolicyConfig]:
            return []

        def _get_quota_allowance(
            self, tenant_ids: dict[str, str | int], query_id: str
        ) -> QuotaAllowance:
            return QuotaAllowance(
                can_run=False,
                max_threads=0,
                explanation={"reason": "policy rejects all queries"},
                is_throttled=True,
                throttle_threshold=100000,
                rejection_threshold=MAX_THRESHOLD,
                quota_used=MAX_THRESHOLD,
                quota_unit=NO_UNITS,
                suggestion=NO_SUGGESTION,
            )

        def _update_quota_balance(
            self,
            tenant_ids: dict[str, str | int],
            query_id: str,
            result_or_error: QueryResultOrError,
        ) -> None:
            nonlocal update_called
            update_called = True
            return

    with mock.patch(
        "snuba.web.delete_query._get_delete_allocation_policies",
        return_value=[RejectPolicy(StorageKey("doesntmatter"), ["a", "b", "c"], {})],
    ):
        with pytest.raises(QueryException) as excinfo:
            _execute_query(
                query=get_delete_query(),
                storage=storage,
                table="search_issues_local_v2",
                cluster_name="cluster_name",
                attribution_info=attr_into,
                query_settings=HTTPQuerySettings(),
            )
        # extra data contains policy failure information
        assert (
            excinfo.value.extra["stats"]["quota_allowance"]["details"]["RejectPolicy"][
                "explanation"
            ]["reason"]
            == "policy rejects all queries"
        )
        cause = excinfo.value.__cause__
        assert isinstance(cause, AllocationPolicyViolations)
        assert "RejectPolicy" in cause.violations
        assert (
            update_called
        ), "update_quota_balance should have been called even though the query was rejected but was not"
