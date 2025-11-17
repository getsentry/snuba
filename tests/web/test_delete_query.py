from __future__ import annotations

from unittest import mock

import pytest

from snuba.attribution import get_app_id
from snuba.attribution.attribution_info import AttributionInfo
from snuba.clickhouse.columns import ColumnSet
from snuba.clickhouse.query import Query
from snuba.configs.configuration import Configuration, ResourceIdentifier
from snuba.datasets.storages.factory import get_writable_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.query.allocation_policies import (
    MAX_THRESHOLD,
    NO_SUGGESTION,
    NO_UNITS,
    AllocationPolicy,
    AllocationPolicyViolations,
    QueryResultOrError,
    QuotaAllowance,
)
from snuba.query.data_source.simple import Table
from snuba.query.dsl import and_cond, column, equals, literal
from snuba.query.query_settings import HTTPQuerySettings
from snuba.web import QueryException
from snuba.web.delete_query import _execute_query, _parse_column_expression


@pytest.mark.clickhouse_db
@pytest.mark.redis_db
def test_delete_query_clickhouse_error() -> None:
    from_clause = Table(
        "eap_items_1_local",
        ColumnSet([]),
        storage_key=StorageKey.EAP_ITEMS,
        allocation_policies=[],
    )

    query = Query(
        from_clause=from_clause,
        condition=and_cond(
            equals(column("organization_id"), literal(10)),
            equals(column("bad_column_name"), literal(3)),
        ),
        on_cluster=None,
        is_delete=True,
    )

    storage = get_writable_storage(StorageKey("eap_items"))
    attr_into = AttributionInfo(
        get_app_id("blah"),
        {"project_id": 123, "referrer": "r"},
        "blah",
        None,
        None,
        None,
    )
    with pytest.raises(QueryException) as excinfo:
        _execute_query(
            query=query,
            storage=storage,
            table="eap_items_1_local",
            cluster_name="cluster_name",
            attribution_info=attr_into,
            query_settings=HTTPQuerySettings(),
        )

    assert "bad_column_name" in excinfo.value.message


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
        def _additional_config_definitions(self) -> list[Configuration]:
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
        return_value=[
            RejectPolicy(ResourceIdentifier(StorageKey("doesntmatter")), ["a", "b", "c"], {})
        ],
    ):
        query = Query(
            from_clause=Table(
                "search_issues_local_v2",
                ColumnSet([]),
                storage_key=StorageKey.SEARCH_ISSUES,
                allocation_policies=[],
            ),
            condition=and_cond(
                equals(column("group_id"), literal(10)),
                equals(column("project_id"), literal(3)),
            ),
            on_cluster=None,
            is_delete=True,
        )

        with pytest.raises(QueryException) as excinfo:
            _execute_query(
                query=query,
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


def test_parse_column_expression_regular_column() -> None:
    """Test that regular column names are parsed correctly."""
    from snuba.query.expressions import Column

    expr = _parse_column_expression("project_id")
    assert isinstance(expr, Column)
    assert expr.column_name == "project_id"


def test_parse_column_expression_map_access() -> None:
    from snuba.query.expressions import SubscriptableReference

    expr = _parse_column_expression("attributes_string_36['group_id']")
    assert isinstance(expr, SubscriptableReference)
    assert expr.column.column_name == "attributes_string_36"
    assert expr.key.value == "group_id"

    # double quotes are also acceptable
    from snuba.query.expressions import SubscriptableReference

    expr = _parse_column_expression('attributes_string_0["event_id"]')
    assert isinstance(expr, SubscriptableReference)
    assert expr.column.column_name == "attributes_string_0"
    assert expr.key.value == "event_id"


def test_parse_column_expression_formats_correctly() -> None:
    from snuba.clickhouse.formatter.query import format_query
    from snuba.web.delete_query import _construct_condition

    conditions = {
        "project_id": [1],
        "attributes_string_36['group_id']": [12345],
    }

    condition_expr = _construct_condition(conditions)

    query = Query(
        from_clause=Table(
            "eap_items_1_local",
            ColumnSet([]),
            storage_key=StorageKey.EAP_ITEMS,
        ),
        condition=condition_expr,
        is_delete=True,
    )

    formatted = format_query(query)
    sql = formatted.get_sql()

    # Should NOT have backticks around the entire map access expression
    assert "`attributes_string_36['group_id']`" not in sql
    # Should have the correct format without backticks
    assert "attributes_string_36['group_id']" in sql
    assert "equals(project_id, 1)" in sql
    assert "equals(attributes_string_36['group_id'], 12345)" in sql
