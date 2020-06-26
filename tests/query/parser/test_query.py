from typing import Any, MutableMapping

import pytest

from snuba import state
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
from snuba.query.logical import OrderBy, OrderByDirection, Query, SelectedExpression
from snuba.query.parser import parse_query
from snuba.query.parser.exceptions import CyclicAliasException

test_cases = [
    pytest.param(
        {
            "selected_columns": ["column1"],
            "groupby": ["column2", "column3"],
            "aggregations": [["test_func", "column4", "test_func_alias"]],
        },
        Query(
            {},
            TableSource("events", ColumnSet([])),
            selected_columns=[
                SelectedExpression("column2", Column("column2", None, "column2")),
                SelectedExpression("column3", Column("column3", None, "column3")),
                SelectedExpression(
                    "test_func_alias",
                    FunctionCall(
                        "test_func_alias",
                        "test_func",
                        (Column("column4", None, "column4"),),
                    ),
                ),
                SelectedExpression("column1", Column("column1", None, "column1")),
            ],
            groupby=[
                Column("column2", None, "column2"),
                Column("column3", None, "column3"),
            ],
        ),
        id="Select composed by select, groupby and aggregations",
    ),
    pytest.param(
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
                SelectedExpression(
                    None,
                    FunctionCall(
                        None, "format_eventid", (Column("event_id", None, "event_id"),)
                    ),
                ),
                SelectedExpression(
                    "platforms",
                    FunctionCall(
                        "platforms", "count", (Column("platform", None, "platform"),)
                    ),
                ),
                SelectedExpression(
                    "uniq_platforms",
                    FunctionCall(
                        "uniq_platforms",
                        "uniq",
                        (Column("platform", None, "platform"),),
                    ),
                ),
                SelectedExpression(
                    "top_platforms",
                    FunctionCall(
                        "top_platforms",
                        "testF",
                        (
                            Column("platform", None, "platform"),
                            Column("field2", None, "field2"),
                        ),
                    ),
                ),
                SelectedExpression(
                    "f1_alias",
                    FunctionCall(
                        "f1_alias",
                        "f1",
                        (
                            Column("column1", None, "column1"),
                            Column("column2", None, "column2"),
                        ),
                    ),
                ),
                SelectedExpression("f2_alias", FunctionCall("f2_alias", "f2", ())),
            ],
            condition=binary_condition(
                None,
                "in",
                SubscriptableReference(
                    "tags[sentry:dist]",
                    Column("tags", None, "tags"),
                    Literal(None, "sentry:dist"),
                ),
                FunctionCall(
                    None, "tuple", (Literal(None, "dist1"), Literal(None, "dist2"),),
                ),
            ),
            having=binary_condition(
                None,
                "greater",
                Column("times_seen", None, "times_seen"),
                Literal(None, 1),
            ),
            groupby=[
                FunctionCall(
                    None, "format_eventid", (Column("event_id", None, "event_id"),)
                )
            ],
        ),
        id="Format a query with functions in all fields",
    ),
    pytest.param(
        {
            "selected_columns": ["column1", "column2"],
            "orderby": ["column1", "-column2", ["-func", ["column3"]]],
        },
        Query(
            {},
            TableSource("events", ColumnSet([])),
            selected_columns=[
                SelectedExpression("column1", Column("column1", None, "column1")),
                SelectedExpression("column2", Column("column2", None, "column2")),
            ],
            condition=None,
            groupby=None,
            having=None,
            order_by=[
                OrderBy(OrderByDirection.ASC, Column("column1", None, "column1")),
                OrderBy(OrderByDirection.DESC, Column("column2", None, "column2")),
                OrderBy(
                    OrderByDirection.DESC,
                    FunctionCall(None, "func", (Column("column3", None, "column3"),)),
                ),
            ],
        ),
        id="Order by with functions",
    ),
    pytest.param(
        {"selected_columns": [], "groupby": "column1", "orderby": "-column1"},
        Query(
            {},
            TableSource("events", ColumnSet([])),
            selected_columns=[
                SelectedExpression("column1", Column("column1", None, "column1"))
            ],
            condition=None,
            groupby=[Column("column1", None, "column1")],
            having=None,
            order_by=[
                OrderBy(OrderByDirection.DESC, Column("column1", None, "column1"))
            ],
        ),
        id="Order and group by provided as string",
    ),
    pytest.param(
        {
            "selected_columns": ["column1", "tags[test]"],
            "groupby": [["f", ["tags[test2]"]]],
        },
        Query(
            {},
            TableSource("events", ColumnSet([])),
            selected_columns=[
                SelectedExpression(
                    name=None,
                    expression=FunctionCall(
                        None,
                        "f",
                        (
                            SubscriptableReference(
                                "tags[test2]",
                                Column("tags", None, "tags"),
                                Literal(None, "test2"),
                            ),
                        ),
                    ),
                ),
                SelectedExpression("column1", Column("column1", None, "column1")),
                SelectedExpression(
                    "tags[test]",
                    SubscriptableReference(
                        "tags[test]",
                        Column("tags", None, "tags"),
                        Literal(None, "test"),
                    ),
                ),
            ],
            groupby=[
                FunctionCall(
                    None,
                    "f",
                    (
                        SubscriptableReference(
                            "tags[test2]",
                            Column("tags", None, "tags"),
                            Literal(None, "test2"),
                        ),
                    ),
                )
            ],
        ),
        id="Unpacks subscriptable references",
    ),
    pytest.param(
        {
            "selected_columns": [
                "group_id",
                ["g", ["something"], "issue_id"],
                ["f", [["z", ["a"]]], "a"],
            ],
            "conditions": [[["f", ["issue_id"], "group_id"], "=", 1]],
            "orderby": ["group_id"],
        },
        Query(
            {},
            TableSource("events", ColumnSet([])),
            selected_columns=[
                SelectedExpression(
                    "group_id",
                    FunctionCall(
                        "group_id",
                        "f",
                        (
                            FunctionCall(
                                "issue_id",
                                "g",
                                (Column("something", None, "something"),),
                            ),
                        ),
                    ),
                ),
                SelectedExpression(
                    "issue_id",
                    FunctionCall(
                        "issue_id", "g", (Column("something", None, "something"),)
                    ),
                ),
                SelectedExpression(
                    "a",
                    FunctionCall(
                        "a", "f", (FunctionCall(None, "z", (Column(None, None, "a"),)),)
                    ),
                ),
            ],
            condition=binary_condition(
                None,
                "equals",
                FunctionCall(
                    "group_id",
                    "f",
                    (
                        FunctionCall(
                            "issue_id", "g", (Column("something", None, "something"),)
                        ),
                    ),
                ),
                Literal(None, 1),
            ),
            order_by=[
                OrderBy(
                    OrderByDirection.ASC,
                    FunctionCall(
                        "group_id",
                        "f",
                        (
                            FunctionCall(
                                "issue_id",
                                "g",
                                (Column("something", None, "something"),),
                            ),
                        ),
                    ),
                ),
            ],
        ),
        id="Alias references are expanded",
    ),
    pytest.param(
        {"selected_columns": [["f", ["column3"], "exp"], ["f", ["column3"], "exp"]]},
        Query(
            {},
            TableSource("events", ColumnSet([])),
            selected_columns=[
                SelectedExpression(
                    "exp",
                    FunctionCall("exp", "f", (Column("column3", None, "column3"),)),
                ),
                SelectedExpression(
                    "exp",
                    FunctionCall("exp", "f", (Column("column3", None, "column3"),)),
                ),
            ],
        ),
        id="Allowed duplicate alias (same expression)",
    ),
]


@pytest.mark.parametrize("query_body, expected_query", test_cases)
def test_format_expressions(
    query_body: MutableMapping[str, Any], expected_query: Query
) -> None:
    state.set_config("query_parsing_expand_aliases", 1)
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


def test_shadowing() -> None:
    state.set_config("query_parsing_enforce_validity", 1)
    state.set_config("query_parsing_expand_aliases", 1)
    with pytest.raises(ValueError):
        parse_query(
            {
                "selected_columns": [
                    ["f1", ["column1", "column2"], "f1_alias"],
                    ["f2", [], "f2_alias"],
                ],
                "aggregations": [
                    ["testF", ["platform", "field2"], "f1_alias"]  # Shadowing!
                ],
            },
            get_dataset("events"),
        )


def test_circular_aliases() -> None:
    state.set_config("query_parsing_enforce_validity", 1)
    state.set_config("query_parsing_expand_aliases", 1)
    with pytest.raises(CyclicAliasException):
        parse_query(
            {
                "selected_columns": [
                    ["f1", ["column1", "f2"], "f1"],
                    ["f2", ["f1"], "f2"],
                ],
            },
            get_dataset("events"),
        )

    with pytest.raises(CyclicAliasException):
        parse_query(
            {"selected_columns": [["f1", [["f2", ["c"], "f2"]], "c"]]},
            get_dataset("events"),
        )
