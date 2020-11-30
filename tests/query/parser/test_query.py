from typing import Any, MutableMapping

import pytest

from snuba import state
from snuba.datasets.entities import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.factory import get_dataset
from snuba.query.conditions import binary_condition, ConditionFunctions
from snuba.query.expressions import (
    Argument,
    Column,
    FunctionCall,
    Lambda,
    Literal,
    SubscriptableReference,
)
from snuba.query import OrderBy, OrderByDirection, SelectedExpression
from snuba.query.data_source.simple import Entity as QueryEntity
from snuba.query.logical import Query
from snuba.query.parser import parse_query
from snuba.query.parser.exceptions import AliasShadowingException, CyclicAliasException

test_cases = [
    pytest.param(
        {
            "selected_columns": ["column1"],
            "groupby": ["column2", "column3"],
            "aggregations": [["test_func", "column4", "test_func_alias"]],
        },
        Query(
            {},
            QueryEntity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()
            ),
            selected_columns=[
                SelectedExpression(
                    "column2", Column("_snuba_column2", None, "column2")
                ),
                SelectedExpression(
                    "column3", Column("_snuba_column3", None, "column3")
                ),
                SelectedExpression(
                    "test_func_alias",
                    FunctionCall(
                        "_snuba_test_func_alias",
                        "test_func",
                        (Column("_snuba_column4", None, "column4"),),
                    ),
                ),
                SelectedExpression(
                    "column1", Column("_snuba_column1", None, "column1")
                ),
            ],
            groupby=[
                Column("_snuba_column2", None, "column2"),
                Column("_snuba_column3", None, "column3"),
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
            QueryEntity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()
            ),
            selected_columns=[
                SelectedExpression(
                    None,
                    FunctionCall(
                        None,
                        "format_eventid",
                        (Column("_snuba_event_id", None, "event_id"),),
                    ),
                ),
                SelectedExpression(
                    "platforms",
                    FunctionCall(
                        "_snuba_platforms",
                        "count",
                        (Column("_snuba_platform", None, "platform"),),
                    ),
                ),
                SelectedExpression(
                    "uniq_platforms",
                    FunctionCall(
                        "_snuba_uniq_platforms",
                        "uniq",
                        (Column("_snuba_platform", None, "platform"),),
                    ),
                ),
                SelectedExpression(
                    "top_platforms",
                    FunctionCall(
                        "_snuba_top_platforms",
                        "testF",
                        (
                            Column("_snuba_platform", None, "platform"),
                            Column("_snuba_field2", None, "field2"),
                        ),
                    ),
                ),
                SelectedExpression(
                    "f1_alias",
                    FunctionCall(
                        "_snuba_f1_alias",
                        "f1",
                        (
                            Column("_snuba_column1", None, "column1"),
                            Column("_snuba_column2", None, "column2"),
                        ),
                    ),
                ),
                SelectedExpression(
                    "f2_alias", FunctionCall("_snuba_f2_alias", "f2", ())
                ),
            ],
            condition=binary_condition(
                "in",
                SubscriptableReference(
                    "_snuba_tags[sentry:dist]",
                    Column("_snuba_tags", None, "tags"),
                    Literal(None, "sentry:dist"),
                ),
                FunctionCall(
                    None, "tuple", (Literal(None, "dist1"), Literal(None, "dist2"),),
                ),
            ),
            having=binary_condition(
                "greater",
                Column("_snuba_times_seen", None, "times_seen"),
                Literal(None, 1),
            ),
            groupby=[
                FunctionCall(
                    None,
                    "format_eventid",
                    (Column("_snuba_event_id", None, "event_id"),),
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
            QueryEntity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()
            ),
            selected_columns=[
                SelectedExpression(
                    "column1", Column("_snuba_column1", None, "column1")
                ),
                SelectedExpression(
                    "column2", Column("_snuba_column2", None, "column2")
                ),
            ],
            condition=None,
            groupby=None,
            having=None,
            order_by=[
                OrderBy(
                    OrderByDirection.ASC, Column("_snuba_column1", None, "column1")
                ),
                OrderBy(
                    OrderByDirection.DESC, Column("_snuba_column2", None, "column2")
                ),
                OrderBy(
                    OrderByDirection.DESC,
                    FunctionCall(
                        None, "func", (Column("_snuba_column3", None, "column3"),)
                    ),
                ),
            ],
        ),
        id="Order by with functions",
    ),
    pytest.param(
        {"selected_columns": [], "groupby": "column1", "orderby": "-column1"},
        Query(
            {},
            QueryEntity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()
            ),
            selected_columns=[
                SelectedExpression("column1", Column("_snuba_column1", None, "column1"))
            ],
            condition=None,
            groupby=[Column("_snuba_column1", None, "column1")],
            having=None,
            order_by=[
                OrderBy(
                    OrderByDirection.DESC, Column("_snuba_column1", None, "column1")
                )
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
            QueryEntity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()
            ),
            selected_columns=[
                SelectedExpression(
                    name=None,
                    expression=FunctionCall(
                        None,
                        "f",
                        (
                            SubscriptableReference(
                                "_snuba_tags[test2]",
                                Column("_snuba_tags", None, "tags"),
                                Literal(None, "test2"),
                            ),
                        ),
                    ),
                ),
                SelectedExpression(
                    "column1", Column("_snuba_column1", None, "column1")
                ),
                SelectedExpression(
                    "tags[test]",
                    SubscriptableReference(
                        "_snuba_tags[test]",
                        Column("_snuba_tags", None, "tags"),
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
                            "_snuba_tags[test2]",
                            Column("_snuba_tags", None, "tags"),
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
            QueryEntity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()
            ),
            selected_columns=[
                SelectedExpression(
                    "group_id",
                    FunctionCall(
                        "_snuba_group_id",
                        "f",
                        (
                            FunctionCall(
                                "_snuba_issue_id",
                                "g",
                                (Column("_snuba_something", None, "something"),),
                            ),
                        ),
                    ),
                ),
                SelectedExpression(
                    "issue_id",
                    FunctionCall(
                        "_snuba_issue_id",
                        "g",
                        (Column("_snuba_something", None, "something"),),
                    ),
                ),
                SelectedExpression(
                    "a",
                    FunctionCall(
                        "_snuba_a",
                        "f",
                        (FunctionCall(None, "z", (Column(None, None, "a"),)),),
                    ),
                ),
            ],
            condition=binary_condition(
                "equals",
                FunctionCall(
                    "_snuba_group_id",
                    "f",
                    (
                        FunctionCall(
                            "_snuba_issue_id",
                            "g",
                            (Column("_snuba_something", None, "something"),),
                        ),
                    ),
                ),
                Literal(None, 1),
            ),
            order_by=[
                OrderBy(
                    OrderByDirection.ASC,
                    FunctionCall(
                        "_snuba_group_id",
                        "f",
                        (
                            FunctionCall(
                                "_snuba_issue_id",
                                "g",
                                (Column("_snuba_something", None, "something"),),
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
            QueryEntity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()
            ),
            selected_columns=[
                SelectedExpression(
                    "exp",
                    FunctionCall(
                        "_snuba_exp", "f", (Column("_snuba_column3", None, "column3"),)
                    ),
                ),
                SelectedExpression(
                    "exp",
                    FunctionCall(
                        "_snuba_exp", "f", (Column("_snuba_column3", None, "column3"),)
                    ),
                ),
            ],
        ),
        id="Allowed duplicate alias (same expression)",
    ),
    pytest.param(
        {"selected_columns": [["f", ["column"], "`exp`"], "`exp`"]},
        Query(
            {},
            QueryEntity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()
            ),
            selected_columns=[
                SelectedExpression(
                    "exp",
                    FunctionCall(
                        "_snuba_exp", "f", (Column("_snuba_column", None, "column"),)
                    ),
                ),
                SelectedExpression(
                    "exp",
                    FunctionCall(
                        "_snuba_exp", "f", (Column("_snuba_column", None, "column"),)
                    ),
                ),
            ],
        ),
        id="De-escape aliases defined by the user",
    ),
    pytest.param(
        {
            "selected_columns": ["exception_stacks.type"],
            "aggregations": [["count", None, "count"]],
            "conditions": [],
            "having": [],
            "groupby": [],
            "arrayjoin": "exception_stacks.type",
        },
        Query(
            {},
            QueryEntity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()
            ),
            selected_columns=[
                SelectedExpression("count", FunctionCall("_snuba_count", "count", ())),
                SelectedExpression(
                    "exception_stacks.type",
                    Column(
                        "_snuba_exception_stacks.type", None, "exception_stacks.type"
                    ),
                ),
            ],
            array_join=Column(None, None, "exception_stacks.type"),
        ),
        id="Format a query with array join",
    ),
    pytest.param(
        {
            "selected_columns": ["exception_stacks.type"],
            "aggregations": [["count", None, "count"]],
            "conditions": [["exception_stacks.type", "LIKE", "Arithmetic%"]],
            "having": [],
            "groupby": [],
        },
        Query(
            {},
            QueryEntity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()
            ),
            selected_columns=[
                SelectedExpression("count", FunctionCall("_snuba_count", "count", ())),
                SelectedExpression(
                    "exception_stacks.type",
                    Column(
                        "_snuba_exception_stacks.type", None, "exception_stacks.type"
                    ),
                ),
            ],
            condition=FunctionCall(
                None,
                "arrayExists",
                (
                    Lambda(
                        None,
                        ("x",),
                        FunctionCall(
                            None,
                            "assumeNotNull",
                            (
                                FunctionCall(
                                    None,
                                    "like",
                                    (
                                        Argument(None, "x"),
                                        Literal(None, "Arithmetic%"),
                                    ),
                                ),
                            ),
                        ),
                    ),
                    Column(
                        "_snuba_exception_stacks.type", None, "exception_stacks.type"
                    ),
                ),
            ),
        ),
        id="Format a query with array field in a condition",
    ),
    pytest.param(
        {
            "selected_columns": ["exception_stacks.type"],
            "aggregations": [["count", None, "count"]],
            "conditions": [["exception_stacks.type", "LIKE", "Arithmetic%"]],
            "having": [],
            "groupby": [],
            "arrayjoin": "exception_stacks.type",
        },
        Query(
            {},
            QueryEntity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()
            ),
            selected_columns=[
                SelectedExpression("count", FunctionCall("_snuba_count", "count", ())),
                SelectedExpression(
                    "exception_stacks.type",
                    Column(
                        "_snuba_exception_stacks.type", None, "exception_stacks.type"
                    ),
                ),
            ],
            array_join=Column(None, None, "exception_stacks.type"),
            condition=FunctionCall(
                None,
                "like",
                (
                    Column(
                        "_snuba_exception_stacks.type", None, "exception_stacks.type"
                    ),
                    Literal(None, "Arithmetic%"),
                ),
            ),
        ),
        id="Format a query with array join field in a condition",
    ),
    pytest.param(
        {
            "selected_columns": [["arrayJoin", ["exception_stacks"]]],
            "aggregations": [["count", None, "count"]],
            "conditions": [["exception_stacks.type", "LIKE", "Arithmetic%"]],
            "having": [],
            "groupby": [],
        },
        Query(
            {},
            QueryEntity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()
            ),
            selected_columns=[
                SelectedExpression("count", FunctionCall("_snuba_count", "count", ())),
                SelectedExpression(
                    None,
                    FunctionCall(
                        None,
                        "arrayJoin",
                        (Column("_snuba_exception_stacks", None, "exception_stacks"),),
                    ),
                ),
            ],
            condition=FunctionCall(
                None,
                "like",
                (
                    Column(
                        "_snuba_exception_stacks.type", None, "exception_stacks.type"
                    ),
                    Literal(None, "Arithmetic%"),
                ),
            ),
        ),
        id="Format a query with array join field in a condition and array join in a function",
    ),
    pytest.param(
        {
            "selected_columns": ["exception_stacks.type"],
            "aggregations": [["count", None, "count"]],
            "conditions": [
                [
                    [
                        "or",
                        [
                            [
                                "equals",
                                ["exception_stacks.type", "'ArithmeticException'"],
                            ],
                            [
                                "equals",
                                ["exception_stacks.type", "'RuntimeException'"],
                            ],
                        ],
                    ],
                    "=",
                    1,
                ],
            ],
            "having": [],
            "groupby": [],
        },
        Query(
            {},
            QueryEntity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()
            ),
            selected_columns=[
                SelectedExpression("count", FunctionCall("_snuba_count", "count", ())),
                SelectedExpression(
                    "exception_stacks.type",
                    Column(
                        "_snuba_exception_stacks.type", None, "exception_stacks.type"
                    ),
                ),
            ],
            condition=binary_condition(
                ConditionFunctions.EQ,
                FunctionCall(
                    None,
                    "or",
                    (
                        FunctionCall(
                            None,
                            "arrayExists",
                            (
                                Lambda(
                                    None,
                                    ("x",),
                                    FunctionCall(
                                        None,
                                        "assumeNotNull",
                                        (
                                            FunctionCall(
                                                None,
                                                "equals",
                                                (
                                                    Argument(None, "x"),
                                                    Literal(
                                                        None, "ArithmeticException",
                                                    ),
                                                ),
                                            ),
                                        ),
                                    ),
                                ),
                                Column(
                                    "_snuba_exception_stacks.type",
                                    None,
                                    "exception_stacks.type",
                                ),
                            ),
                        ),
                        FunctionCall(
                            None,
                            "arrayExists",
                            (
                                Lambda(
                                    None,
                                    ("x",),
                                    FunctionCall(
                                        None,
                                        "assumeNotNull",
                                        (
                                            FunctionCall(
                                                None,
                                                "equals",
                                                (
                                                    Argument(None, "x"),
                                                    Literal(None, "RuntimeException",),
                                                ),
                                            ),
                                        ),
                                    ),
                                ),
                                Column(
                                    "_snuba_exception_stacks.type",
                                    None,
                                    "exception_stacks.type",
                                ),
                            ),
                        ),
                    ),
                ),
                Literal(None, 1),
            ),
        ),
        id="Format a query with array field in a boolean condition",
    ),
    pytest.param(
        {
            "selected_columns": ["exception_stacks.type"],
            "aggregations": [["count", None, "count"]],
            "conditions": [
                [
                    [
                        "or",
                        [
                            [
                                "equals",
                                ["exception_stacks.type", "'ArithmeticException'"],
                            ],
                            [
                                "equals",
                                ["exception_stacks.type", "'RuntimeException'"],
                            ],
                        ],
                    ],
                    "=",
                    1,
                ],
            ],
            "having": [],
            "groupby": [],
            "arrayjoin": "exception_stacks",
        },
        Query(
            {},
            QueryEntity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()
            ),
            selected_columns=[
                SelectedExpression("count", FunctionCall("_snuba_count", "count", ())),
                SelectedExpression(
                    "exception_stacks.type",
                    Column(
                        "_snuba_exception_stacks.type", None, "exception_stacks.type"
                    ),
                ),
            ],
            condition=binary_condition(
                ConditionFunctions.EQ,
                FunctionCall(
                    None,
                    "or",
                    (
                        FunctionCall(
                            None,
                            "equals",
                            (
                                Column(
                                    "_snuba_exception_stacks.type",
                                    None,
                                    "exception_stacks.type",
                                ),
                                Literal(None, "ArithmeticException"),
                            ),
                        ),
                        FunctionCall(
                            None,
                            "equals",
                            (
                                Column(
                                    "_snuba_exception_stacks.type",
                                    None,
                                    "exception_stacks.type",
                                ),
                                Literal(None, "RuntimeException"),
                            ),
                        ),
                    ),
                ),
                Literal(None, 1),
            ),
            array_join=Column(None, None, "exception_stacks"),
        ),
        id="Format a query with array join field in a boolean condition",
    ),
    pytest.param(
        {
            "selected_columns": [["arrayJoin", ["exception_stacks.type"]]],
            "aggregations": [["count", None, "count"]],
            "conditions": [
                [
                    [
                        "or",
                        [
                            [
                                "equals",
                                ["exception_stacks.type", "'ArithmeticException'"],
                            ],
                            [
                                "equals",
                                ["exception_stacks.type", "'RuntimeException'"],
                            ],
                        ],
                    ],
                    "=",
                    1,
                ],
            ],
            "having": [],
            "groupby": [],
        },
        Query(
            {},
            QueryEntity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()
            ),
            selected_columns=[
                SelectedExpression("count", FunctionCall("_snuba_count", "count", ())),
                SelectedExpression(
                    None,
                    FunctionCall(
                        None,
                        "arrayJoin",
                        (
                            Column(
                                "_snuba_exception_stacks.type",
                                None,
                                "exception_stacks.type",
                            ),
                        ),
                    ),
                ),
            ],
            condition=binary_condition(
                ConditionFunctions.EQ,
                FunctionCall(
                    None,
                    "or",
                    (
                        FunctionCall(
                            None,
                            "equals",
                            (
                                Column(
                                    "_snuba_exception_stacks.type",
                                    None,
                                    "exception_stacks.type",
                                ),
                                Literal(None, "ArithmeticException"),
                            ),
                        ),
                        FunctionCall(
                            None,
                            "equals",
                            (
                                Column(
                                    "_snuba_exception_stacks.type",
                                    None,
                                    "exception_stacks.type",
                                ),
                                Literal(None, "RuntimeException"),
                            ),
                        ),
                    ),
                ),
                Literal(None, 1),
            ),
        ),
        id="Format a query with array join field in a boolean condition and array join in a function",
    ),
    pytest.param(
        {
            "aggregations": [["count", None, "count"]],
            "conditions": [
                [
                    [
                        "or",
                        [
                            ["equals", [["ifNull", ["tags[foo]", "''"]], "'baz'"]],
                            ["equals", [["ifNull", ["tags[foo.bar]", "''"]], "'qux'"]],
                        ],
                    ],
                    "=",
                    1,
                ],
            ],
            "having": [],
            "groupby": ["tags_key"],
        },
        Query(
            {},
            QueryEntity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()
            ),
            selected_columns=[
                SelectedExpression(
                    name="tags_key",
                    expression=Column("_snuba_tags_key", None, "tags_key"),
                ),
                SelectedExpression("count", FunctionCall("_snuba_count", "count", ())),
            ],
            groupby=[Column("_snuba_tags_key", None, "tags_key")],
            condition=binary_condition(
                ConditionFunctions.EQ,
                FunctionCall(
                    None,
                    "or",
                    (
                        FunctionCall(
                            None,
                            "equals",
                            (
                                FunctionCall(
                                    None,
                                    "ifNull",
                                    (
                                        SubscriptableReference(
                                            "_snuba_tags[foo]",
                                            Column("_snuba_tags", None, "tags"),
                                            Literal(None, "foo"),
                                        ),
                                        Literal(None, ""),
                                    ),
                                ),
                                Literal(None, "baz"),
                            ),
                        ),
                        FunctionCall(
                            None,
                            "equals",
                            (
                                FunctionCall(
                                    None,
                                    "ifNull",
                                    (
                                        SubscriptableReference(
                                            "_snuba_tags[foo.bar]",
                                            Column("_snuba_tags", None, "tags"),
                                            Literal(None, "foo.bar"),
                                        ),
                                        Literal(None, ""),
                                    ),
                                ),
                                Literal(None, "qux"),
                            ),
                        ),
                    ),
                ),
                Literal(None, 1),
            ),
        ),
        id="Format a query with array column nested in function",
    ),
]


@pytest.mark.parametrize("query_body, expected_query", test_cases)
def test_format_expressions(
    query_body: MutableMapping[str, Any], expected_query: Query
) -> None:
    state.set_config("query_parsing_expand_aliases", 1)
    events = get_dataset("events")
    query = parse_query(query_body, events)

    eq, reason = query.equals(expected_query)
    assert eq, reason


def test_shadowing() -> None:
    with pytest.raises(AliasShadowingException):
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
