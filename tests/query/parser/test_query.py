from datetime import datetime
from typing import Any, MutableMapping, Optional

import pytest
from snuba_sdk.legacy import json_to_snql

from snuba import state
from snuba.datasets.entities import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.factory import get_dataset
from snuba.query import OrderBy, OrderByDirection, SelectedExpression
from snuba.query.conditions import (
    BooleanFunctions,
    ConditionFunctions,
    binary_condition,
)
from snuba.query.data_source.simple import Entity as QueryEntity
from snuba.query.expressions import (
    Argument,
    Column,
    Expression,
    FunctionCall,
    Lambda,
    Literal,
    SubscriptableReference,
)
from snuba.query.logical import Query
from snuba.query.parser.exceptions import AliasShadowingException, CyclicAliasException
from snuba.query.snql.parser import parse_snql_query


def with_required(condition: Optional[Expression] = None) -> Expression:
    required = binary_condition(
        BooleanFunctions.AND,
        FunctionCall(
            None,
            "greaterOrEquals",
            (
                Column("_snuba_timestamp", None, "timestamp"),
                Literal(None, datetime(2021, 1, 1, 0, 0)),
            ),
        ),
        binary_condition(
            BooleanFunctions.AND,
            FunctionCall(
                None,
                "less",
                (
                    Column("_snuba_timestamp", None, "timestamp"),
                    Literal(None, datetime(2021, 1, 2, 0, 0)),
                ),
            ),
            FunctionCall(
                None,
                "equals",
                (Column("_snuba_project_id", None, "project_id"), Literal(None, 1),),
            ),
        ),
    )

    if condition:
        return binary_condition(BooleanFunctions.AND, condition, required)

    return required


test_cases = [
    pytest.param(
        {
            "selected_columns": ["column1"],
            "groupby": ["column2", "column3"],
            "aggregations": [["test_func", "column4", "test_func_alias"]],
        },
        Query(
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
            condition=with_required(),
            limit=1000,
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
            QueryEntity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()
            ),
            selected_columns=[
                SelectedExpression(
                    "format_eventid(event_id)",
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
            condition=with_required(
                binary_condition(
                    "in",
                    SubscriptableReference(
                        "_snuba_tags[sentry:dist]",
                        Column("_snuba_tags", None, "tags"),
                        Literal(None, "sentry:dist"),
                    ),
                    FunctionCall(
                        None,
                        "tuple",
                        (Literal(None, "dist1"), Literal(None, "dist2"),),
                    ),
                )
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
            limit=1000,
        ),
        id="Format a query with functions in all fields",
    ),
    pytest.param(
        {
            "selected_columns": ["column1", "column2"],
            "orderby": ["column1", "-column2", ["-func", ["column3"]]],
        },
        Query(
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
            condition=with_required(),
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
            limit=1000,
        ),
        id="Order by with functions",
    ),
    pytest.param(
        {"selected_columns": ["column1"], "groupby": "column1", "orderby": "-column1"},
        Query(
            QueryEntity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()
            ),
            selected_columns=[
                SelectedExpression(
                    "column1", Column("_snuba_column1", None, "column1")
                ),
                SelectedExpression(
                    "column1", Column("_snuba_column1", None, "column1")
                ),
            ],
            condition=with_required(),
            groupby=[Column("_snuba_column1", None, "column1")],
            having=None,
            order_by=[
                OrderBy(
                    OrderByDirection.DESC, Column("_snuba_column1", None, "column1")
                )
            ],
            limit=1000,
        ),
        id="Order and group by provided as string",
    ),
    pytest.param(
        {
            "selected_columns": ["column1", "tags[test]"],
            "groupby": [["foo", ["tags[test2]"]]],
        },
        Query(
            QueryEntity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()
            ),
            selected_columns=[
                SelectedExpression(
                    name="foo(tags[test2])",
                    expression=FunctionCall(
                        None,
                        "foo",
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
                    "foo",
                    (
                        SubscriptableReference(
                            "_snuba_tags[test2]",
                            Column("_snuba_tags", None, "tags"),
                            Literal(None, "test2"),
                        ),
                    ),
                )
            ],
            condition=with_required(),
            limit=1000,
        ),
        id="Unpacks subscriptable references",
    ),
    pytest.param(
        {
            "selected_columns": [
                "group_id",
                ["goo", ["something"], "issue_id"],
                ["foo", [["zoo", ["a"]]], "a"],
            ],
            "conditions": [[["foo", ["issue_id"], "group_id"], "=", 1]],
            "orderby": ["group_id"],
        },
        Query(
            QueryEntity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()
            ),
            selected_columns=[
                SelectedExpression(
                    "group_id",
                    FunctionCall(
                        "_snuba_group_id",
                        "foo",
                        (
                            FunctionCall(
                                "_snuba_issue_id",
                                "goo",
                                (Column("_snuba_something", None, "something"),),
                            ),
                        ),
                    ),
                ),
                SelectedExpression(
                    "issue_id",
                    FunctionCall(
                        "_snuba_issue_id",
                        "goo",
                        (Column("_snuba_something", None, "something"),),
                    ),
                ),
                SelectedExpression(
                    "a",
                    FunctionCall(
                        "_snuba_a",
                        "foo",
                        (FunctionCall(None, "zoo", (Column(None, None, "a"),)),),
                    ),
                ),
            ],
            condition=with_required(
                binary_condition(
                    "equals",
                    FunctionCall(
                        "_snuba_group_id",
                        "foo",
                        (
                            FunctionCall(
                                "_snuba_issue_id",
                                "goo",
                                (Column("_snuba_something", None, "something"),),
                            ),
                        ),
                    ),
                    Literal(None, 1),
                )
            ),
            order_by=[
                OrderBy(
                    OrderByDirection.ASC,
                    FunctionCall(
                        "_snuba_group_id",
                        "foo",
                        (
                            FunctionCall(
                                "_snuba_issue_id",
                                "goo",
                                (Column("_snuba_something", None, "something"),),
                            ),
                        ),
                    ),
                ),
            ],
            limit=1000,
        ),
        id="Alias references are expanded",
    ),
    pytest.param(
        {
            "selected_columns": [
                ["foo", ["column3"], "exp"],
                ["foo", ["column3"], "exp"],
            ]
        },
        Query(
            QueryEntity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()
            ),
            selected_columns=[
                SelectedExpression(
                    "exp",
                    FunctionCall(
                        "_snuba_exp",
                        "foo",
                        (Column("_snuba_column3", None, "column3"),),
                    ),
                ),
                SelectedExpression(
                    "exp",
                    FunctionCall(
                        "_snuba_exp",
                        "foo",
                        (Column("_snuba_column3", None, "column3"),),
                    ),
                ),
            ],
            condition=with_required(),
            limit=1000,
        ),
        id="Allowed duplicate alias (same expression)",
    ),
    pytest.param(
        {"selected_columns": [["foo", ["column"], "exp"], "exp"]},
        Query(
            QueryEntity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()
            ),
            selected_columns=[
                SelectedExpression(
                    "exp",
                    FunctionCall(
                        "_snuba_exp", "foo", (Column("_snuba_column", None, "column"),)
                    ),
                ),
                SelectedExpression(
                    "exp",
                    FunctionCall(
                        "_snuba_exp", "foo", (Column("_snuba_column", None, "column"),)
                    ),
                ),
            ],
            condition=with_required(),
            limit=1000,
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
            array_join=Column("exception_stacks.type", None, "exception_stacks.type"),
            condition=with_required(),
            limit=1000,
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
            condition=with_required(
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
                            "_snuba_exception_stacks.type",
                            None,
                            "exception_stacks.type",
                        ),
                    ),
                )
            ),
            limit=1000,
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
            array_join=Column("exception_stacks.type", None, "exception_stacks.type"),
            condition=with_required(
                FunctionCall(
                    None,
                    "like",
                    (
                        Column(
                            "_snuba_exception_stacks.type",
                            None,
                            "exception_stacks.type",
                        ),
                        Literal(None, "Arithmetic%"),
                    ),
                )
            ),
            limit=1000,
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
            QueryEntity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()
            ),
            selected_columns=[
                SelectedExpression("count", FunctionCall("_snuba_count", "count", ())),
                SelectedExpression(
                    "arrayJoin(exception_stacks)",
                    FunctionCall(
                        None,
                        "arrayJoin",
                        (Column("_snuba_exception_stacks", None, "exception_stacks"),),
                    ),
                ),
            ],
            condition=with_required(
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
                            "_snuba_exception_stacks.type",
                            None,
                            "exception_stacks.type",
                        ),
                    ),
                ),
            ),
            limit=1000,
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
            condition=with_required(
                binary_condition(
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
                                                        Literal(
                                                            None, "RuntimeException",
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
                        ),
                    ),
                    Literal(None, 1),
                )
            ),
            limit=1000,
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
            condition=with_required(
                binary_condition(
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
                )
            ),
            limit=1000,
            array_join=Column("exception_stacks", None, "exception_stacks"),
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
            QueryEntity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()
            ),
            selected_columns=[
                SelectedExpression("count", FunctionCall("_snuba_count", "count", ())),
                SelectedExpression(
                    "arrayJoin(exception_stacks.type)",
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
            condition=with_required(
                binary_condition(
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
                                                        Literal(
                                                            None, "RuntimeException",
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
                        ),
                    ),
                    Literal(alias=None, value=1),
                ),
            ),
            limit=1000,
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
            condition=with_required(
                binary_condition(
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
                )
            ),
            limit=1000,
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
    # HACK until we migrate these tests to SnQL
    if not query_body.get("conditions"):
        query_body["conditions"] = []
    query_body["conditions"] += [
        ["timestamp", ">=", "2021-01-01T00:00:00"],
        ["timestamp", "<", "2021-01-02T00:00:00"],
        ["project_id", "=", 1],
    ]
    snql_query = json_to_snql(query_body, "events")
    query = parse_snql_query(str(snql_query), events)

    eq, reason = query.equals(expected_query)
    assert eq, reason


def test_shadowing() -> None:
    with pytest.raises(AliasShadowingException):
        parse_snql_query(
            """
            MATCH (events)
            SELECT f1(column1, column2) AS f1_alias, f2() AS f2_alias, testF(platform, field2) AS f1_alias
            WHERE project_id = 1
            AND timestamp >= toDateTime('2020-01-01 12:00:00')
            AND timestamp < toDateTime('2020-01-02 12:00:00')
            """,
            get_dataset("events"),
        )


def test_circular_aliases() -> None:
    with pytest.raises(CyclicAliasException):
        parse_snql_query(
            """
            MATCH (events)
            SELECT f1(column1, f2) AS f1, f2(f1) AS f2
            WHERE project_id = 1
            AND timestamp >= toDateTime('2020-01-01 12:00:00')
            AND timestamp < toDateTime('2020-01-02 12:00:00')
            """,
            get_dataset("events"),
        )

    with pytest.raises(CyclicAliasException):
        parse_snql_query(
            """
            MATCH (events)
            SELECT f1(f2(c) AS f2) AS c
            WHERE project_id = 1
            AND timestamp >= toDateTime('2020-01-01 12:00:00')
            AND timestamp < toDateTime('2020-01-02 12:00:00')
            """,
            get_dataset("events"),
        )
