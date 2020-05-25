import pytest

from typing import Any, MutableMapping

from snuba.clickhouse.columns import ColumnSet
from snuba.datasets.factory import get_dataset
from snuba.datasets.schemas.tables import TableSource
from snuba.query.conditions import binary_condition
from snuba.query.expressions import (
    Column,
    FunctionCall,
    Literal,
    SubscriptableReference,
)
from snuba.query.logical import OrderBy, OrderByDirection, Query
from snuba.query.parser import build_query


test_cases = [
    (
        {
            "selected_columns": ["event_id"],
            "groupby": ["group_id", "message"],
            "aggregations": [["test_func", "level", "test_func_alias"]],
        },
        Query(
            {},
            TableSource("events", ColumnSet([])),
            selected_columns=[
                Column(None, None, "group_id"),
                Column(None, None, "message"),
                FunctionCall(
                    "test_func_alias", "test_func", (Column(None, None, "level"),)
                ),
                Column(None, None, "event_id"),
            ],
            groupby=[Column(None, None, "group_id"), Column(None, None, "message")],
        ),
    ),  # Basic SELECT composed through the selectedcols and aggregations
    (
        {
            "selected_columns": [
                ["f1", ["level", "group_id"], "f1_alias"],
                ["f2", [], "f2_alias"],
            ],
            "aggregations": [
                ["count", "platform", "platforms"],
                ["uniq", "platform", "uniq_platforms"],
                ["testF", ["platform", "message"], "top_platforms"],
            ],
            "conditions": [["tags[sentry:dist]", "IN", ["dist1", "dist2"]]],
            "having": [["timestamp", ">", 1]],
            "groupby": [["format_eventid", ["event_id"]]],
        },
        Query(
            {},
            TableSource("events", ColumnSet([])),
            selected_columns=[
                FunctionCall(None, "format_eventid", (Column(None, None, "event_id"),)),
                FunctionCall("platforms", "count", (Column(None, None, "platform"),)),
                FunctionCall(
                    "uniq_platforms", "uniq", (Column(None, None, "platform"),)
                ),
                FunctionCall(
                    "top_platforms",
                    "testF",
                    (Column(None, None, "platform"), Column(None, None, "message")),
                ),
                FunctionCall(
                    "f1_alias",
                    "f1",
                    (Column(None, None, "level"), Column(None, None, "group_id")),
                ),
                FunctionCall("f2_alias", "f2", ()),
            ],
            condition=binary_condition(
                None,
                "in",
                SubscriptableReference(
                    "tags[sentry:dist]",
                    Column(None, None, "tags"),
                    Literal(None, "sentry:dist"),
                ),
                FunctionCall(
                    None, "tuple", (Literal(None, "dist1"), Literal(None, "dist2"),),
                ),
            ),
            having=binary_condition(
                None, "greater", Column(None, None, "timestamp"), Literal(None, 1)
            ),
            groupby=[
                FunctionCall(None, "format_eventid", (Column(None, None, "event_id"),))
            ],
        ),
    ),  # Format a query with functions in all fields
    (
        {
            "selected_columns": ["event_id", "time"],
            "orderby": ["event_id", "-time", ["-func", ["level"]]],
        },
        Query(
            {},
            TableSource("events", ColumnSet([])),
            selected_columns=[
                Column(None, None, "event_id"),
                Column(None, None, "time"),
            ],
            condition=None,
            groupby=None,
            having=None,
            order_by=[
                OrderBy(OrderByDirection.ASC, Column(None, None, "event_id")),
                OrderBy(OrderByDirection.DESC, Column(None, None, "time")),
                OrderBy(
                    OrderByDirection.DESC,
                    FunctionCall(None, "func", (Column(None, None, "level"),)),
                ),
            ],
        ),
    ),  # Order by with functions
    (
        {"selected_columns": [], "groupby": "timestamp", "orderby": "-timestamp"},
        Query(
            {},
            TableSource("events", ColumnSet([])),
            selected_columns=[Column(None, None, "timestamp")],
            condition=None,
            groupby=[Column(None, None, "timestamp")],
            having=None,
            order_by=[OrderBy(OrderByDirection.DESC, Column(None, None, "timestamp"))],
        ),
    ),  # Order and group by provided as string
    (
        {
            "selected_columns": ["event_id", "tags[test]"],
            "groupby": [["f", ["tags[test2]"]]],
        },
        Query(
            {},
            TableSource("events", ColumnSet([])),
            selected_columns=[
                FunctionCall(
                    None,
                    "f",
                    (
                        SubscriptableReference(
                            "tags[test2]",
                            Column(None, None, "tags"),
                            Literal(None, "test2"),
                        ),
                    ),
                ),
                Column(None, None, "event_id"),
                SubscriptableReference(
                    "tags[test]", Column(None, None, "tags"), Literal(None, "test")
                ),
            ],
            groupby=[
                FunctionCall(
                    None,
                    "f",
                    (
                        SubscriptableReference(
                            "tags[test2]",
                            Column(None, None, "tags"),
                            Literal(None, "test2"),
                        ),
                    ),
                )
            ],
        ),
    ),  # Unpack nested column both in a simple expression and in a function call.
]


@pytest.mark.parametrize("query_body, expected_query", test_cases)
def test_format_expressions(
    query_body: MutableMapping[str, Any], expected_query: Query
) -> None:
    events = get_dataset("events")
    query = build_query(query_body, events)

    # We cannot just run == on the query objects. The content of the two
    # objects is different, being one the AST and the ont the AST + raw body
    assert (
        query.get_selected_columns_from_ast()
        == expected_query.get_selected_columns_from_ast()
    )
    assert query.get_groupby_from_ast() == expected_query.get_groupby_from_ast()
    assert query.get_condition_from_ast() == expected_query.get_condition_from_ast()
    assert query.get_arrayjoin_from_ast() == expected_query.get_arrayjoin_from_ast()
    assert query.get_having_from_ast() == expected_query.get_having_from_ast()
    assert query.get_orderby_from_ast() == expected_query.get_orderby_from_ast()
