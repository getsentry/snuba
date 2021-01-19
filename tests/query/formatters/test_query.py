from typing import Union

import pytest
from snuba.datasets.entities import EntityKey
from snuba.query import LimitBy, OrderBy, OrderByDirection, SelectedExpression
from snuba.query.composite import CompositeQuery
from snuba.query.conditions import binary_condition
from snuba.query.data_source.join import (
    IndividualNode,
    JoinClause,
    JoinCondition,
    JoinConditionExpression,
    JoinType,
)
from snuba.query.data_source.simple import Entity
from snuba.query.expressions import Column, FunctionCall, Literal
from snuba.query.formatters.tracing import TExpression, format_query
from snuba.query.logical import Query as LogicalQuery
from tests.query.joins.equivalence_schema import (
    EVENTS_SCHEMA,
    GROUPS_SCHEMA,
)

BASIC_JOIN = JoinClause(
    left_node=IndividualNode(
        alias="ev", data_source=Entity(EntityKey.EVENTS, EVENTS_SCHEMA, None),
    ),
    right_node=IndividualNode(
        alias="gr", data_source=Entity(EntityKey.GROUPEDMESSAGES, GROUPS_SCHEMA, None),
    ),
    keys=[
        JoinCondition(
            left=JoinConditionExpression("ev", "group_id"),
            right=JoinConditionExpression("gr", "id"),
        )
    ],
    join_type=JoinType.INNER,
)

LOGICAL_QUERY = LogicalQuery(
    {},
    from_clause=Entity(EntityKey.EVENTS, EVENTS_SCHEMA, 0.5),
    selected_columns=[
        SelectedExpression("c1", Column("_snuba_c1", "t", "c")),
        SelectedExpression(
            "f1", FunctionCall("_snuba_f1", "f", (Column(None, "t", "c2"),))
        ),
    ],
    array_join=Column(None, None, "col"),
    condition=binary_condition(
        "equals", Column(None, None, "c4"), Literal(None, "asd")
    ),
    groupby=[Column(None, "t", "c4")],
    having=binary_condition("equals", Column(None, None, "c6"), Literal(None, "asd2")),
    order_by=[OrderBy(OrderByDirection.ASC, Column(None, "t", "c"))],
    limitby=LimitBy(100, Column(None, None, "c8")),
    limit=150,
)

SIMPLE_FORMATTED = {
    "FROM": {"ENTITY": EntityKey.EVENTS, "SAMPLE": "0.5"},
    "SELECT": [["c1", "(t.c AS _snuba_c1)"], ["f1", ["_snuba_f1", "f", ["t.c2"]]]],
    "ARRAYJOIN": "col",
    "WHERE": ["equals", ["c4", "asd"]],
    "GROUPBY": ["t.c4"],
    "HAVING": ["equals", ["c6", "asd2"]],
    "ORDERBY": [["t.c", OrderByDirection.ASC]],
    "LIMITBY": {"LIMIT": 100, "BY": "c8"},
    "LIMIT": 150,
}


TEST_JOIN = [
    pytest.param(LOGICAL_QUERY, SIMPLE_FORMATTED, id="Simple logical query",),
    pytest.param(
        CompositeQuery(
            from_clause=LOGICAL_QUERY,
            selected_columns=[
                SelectedExpression(
                    "f", FunctionCall("f", "avg", (Column(None, "t", "c"),))
                )
            ],
        ),
        {
            "FROM": SIMPLE_FORMATTED,
            "SELECT": [["f", ["f", "avg", ["t.c"]]]],
            "GROUPBY": [],
            "ORDERBY": [],
        },
        id="Nested Query",
    ),
    pytest.param(
        CompositeQuery(
            from_clause=BASIC_JOIN,
            selected_columns=[
                SelectedExpression("c1", Column("_snuba_c1", "ev", "c")),
                SelectedExpression(
                    "f1", FunctionCall("_snuba_f1", "f", (Column(None, "ev", "c2"),))
                ),
            ],
        ),
        {
            "FROM": {
                "LEFT": ["ev", {"ENTITY": EntityKey.EVENTS}],
                "TYPE": JoinType.INNER,
                "RIGHT": ["gr", {"ENTITY": EntityKey.GROUPEDMESSAGES}],
                "ON": [["ev.group_id", "gr.id"]],
            },
            "SELECT": [
                ["c1", "(ev.c AS _snuba_c1)"],
                ["f1", ["_snuba_f1", "f", ["ev.c2"]]],
            ],
            "GROUPBY": [],
            "ORDERBY": [],
        },
        id="Basic Join",
    ),
]


@pytest.mark.parametrize("query, formatted", TEST_JOIN)
def test_query_formatter(
    query: Union[LogicalQuery, CompositeQuery[Entity]], formatted: TExpression
) -> None:
    assert format_query(query) == formatted
