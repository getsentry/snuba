import pytest

from typing import Any, MutableMapping

from snuba.clickhouse.columns import ColumnSet
from snuba.datasets.factory import get_dataset
from snuba.datasets.schemas.tables import TableSource
from snuba.query.conditions import binary_condition
from snuba.query.expressions import Column, FunctionCall, Literal
from snuba.query.parser import parse_query
from snuba.query.query import OrderBy, OrderByDirection, Query

test_cases = [
    (
        {
            "selected_columns": ["column1"],
            "groupby": ["column2", "column3"],
            "aggregations": [["test_func", "column4", "test_func_alias"]],
        },
        Query(
            {},
            TableSource("events", ColumnSet([])),
            selected_columns=[
                Column(None, "column2", None),
                Column(None, "column3", None),
                FunctionCall(
                    "test_func_alias", "test_func", (Column(None, "column4", None),)
                ),
                Column(None, "column1", None),
            ],
            groupby=[Column(None, "column2", None), Column(None, "column3", None)],
        ),
    ),  # Basic SELECT composed through the selectedcols and aggregations
    (
        {
            "selected_columns": [
                ["f1", ["column1", "column2"], "f1_alias"],
                ["f2", [], "f2_alias"],
            ],
            "aggregations": [
                ["count", "platform", "platforms"],
                ["uniq", "platform", "uniq_platforms"],
                ["testF", ["platform", "field2"], "top_platforms"],
            ],
            "conditions": [["tags[sentry:dist]", "IN", ["dist1", "dist2"]]],
            "having": [["times_seen", ">", 1]],
            "groupby": [["format_eventid", ["event_id"]]],
        },
        Query(
            {},
            TableSource("events", ColumnSet([])),
            selected_columns=[
                FunctionCall(None, "format_eventid", (Column(None, "event_id", None),)),
                FunctionCall("platforms", "count", (Column(None, "platform", None),)),
                FunctionCall(
                    "uniq_platforms", "uniq", (Column(None, "platform", None),)
                ),
                FunctionCall(
                    "top_platforms",
                    "testF",
                    (Column(None, "platform", None), Column(None, "field2", None)),
                ),
                FunctionCall(
                    "f1_alias",
                    "f1",
                    (Column(None, "column1", None), Column(None, "column2", None)),
                ),
                FunctionCall("f2_alias", "f2", ()),
            ],
            condition=binary_condition(
                None,
                "in",
                Column(None, "tags[sentry:dist]", None),
                FunctionCall(
                    None, "tuple", (Literal(None, "dist1"), Literal(None, "dist2"),),
                ),
            ),
            having=binary_condition(
                None, "greater", Column(None, "times_seen", None), Literal(None, 1)
            ),
            groupby=[
                FunctionCall(None, "format_eventid", (Column(None, "event_id", None),))
            ],
        ),
    ),  # Format a query with functions in all fields
    (
        {
            "selected_columns": ["column1", "column2"],
            "orderby": ["column1", "-column2", ["-func", ["column3"]]],
        },
        Query(
            {},
            TableSource("events", ColumnSet([])),
            selected_columns=[
                Column(None, "column1", None),
                Column(None, "column2", None),
            ],
            condition=None,
            groupby=None,
            having=None,
            order_by=[
                OrderBy(OrderByDirection.ASC, Column(None, "column1", None)),
                OrderBy(OrderByDirection.DESC, Column(None, "column2", None)),
                OrderBy(
                    OrderByDirection.DESC,
                    FunctionCall(None, "func", (Column(None, "column3", None),)),
                ),
            ],
        ),
    ),  # Order by with functions
    (
        {"selected_columns": [], "groupby": "column1", "orderby": "-column1"},
        Query(
            {},
            TableSource("events", ColumnSet([])),
            selected_columns=[Column(None, "column1", None)],
            condition=None,
            groupby=[Column(None, "column1", None)],
            having=None,
            order_by=[OrderBy(OrderByDirection.DESC, Column(None, "column1", None))],
        ),
    ),  # Order and group by provided as string
]


@pytest.mark.parametrize("query_body, expected_query", test_cases)
def test_format_expressions(
    query_body: MutableMapping[str, Any], expected_query: Query
) -> None:
    events = get_dataset("events")
    query = parse_query(query_body, events)

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
