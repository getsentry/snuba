from functools import partial

import pytest
from snuba.datasets.entities import EntityKey
from snuba.datasets.entities.factory import ENTITY_IMPL
from snuba.query import SelectedExpression
from snuba.query.composite import CompositeQuery
from snuba.query.conditions import (
    BooleanFunctions,
    ConditionFunctions,
    binary_condition,
    combine_and_conditions,
)
from snuba.query.data_source.join import (
    IndividualNode,
    JoinClause,
    JoinCondition,
    JoinConditionExpression,
    JoinType,
)
from snuba.query.data_source.simple import Entity as EntitySource
from snuba.query.expressions import Column, Expression, FunctionCall, Literal
from snuba.query.joins.equivalence_adder import (
    _classify_single_column_condition,
    _replace_col,
    add_equivalent_conditions,
)
from snuba.query.joins.pre_processor import QualifiedCol
from tests.query.joins.equivalence_schema import (
    EVENTS_SCHEMA,
    GROUPS_SCHEMA,
    Events,
    GroupedMessage,
)


def test_classify_and_replace() -> None:
    condition = binary_condition(
        ConditionFunctions.EQ, Column(None, "ev", "project_id"), Literal(None, 1)
    )
    assert _classify_single_column_condition(
        condition, {"ev": EntityKey.EVENTS}
    ) == QualifiedCol(EntityKey.EVENTS, "project_id")

    assert condition.transform(
        partial(
            _replace_col,
            {"ev": EntityKey.EVENTS, "gr": EntityKey.GROUPEDMESSAGES},
            {EntityKey.EVENTS: "ev", EntityKey.GROUPEDMESSAGES: "gr"},
            QualifiedCol(EntityKey.EVENTS, "project_id"),
            QualifiedCol(EntityKey.GROUPEDMESSAGES, "project_id"),
        )
    ) == binary_condition(
        ConditionFunctions.EQ, Column(None, "gr", "project_id"), Literal(None, 1)
    )


ENTITY_GROUP_JOIN = JoinClause(
    IndividualNode("ev", EntitySource(EntityKey.EVENTS, EVENTS_SCHEMA, None)),
    IndividualNode("gr", EntitySource(EntityKey.GROUPEDMESSAGES, GROUPS_SCHEMA, None)),
    [
        JoinCondition(
            JoinConditionExpression("ev", "group_id"),
            JoinConditionExpression("gr", "id"),
        )
    ],
    JoinType.INNER,
    None,
)


TEST_REPLACEMENT = [
    pytest.param(
        binary_condition(
            ConditionFunctions.EQ, Column(None, "ev", "event_id"), Literal(None, 1)
        ),
        ENTITY_GROUP_JOIN,
        binary_condition(
            ConditionFunctions.EQ, Column(None, "ev", "event_id"), Literal(None, 1)
        ),
        id="No condition to add",
    ),
    pytest.param(
        binary_condition(
            ConditionFunctions.EQ, Column(None, "ev", "project_id"), Literal(None, 1)
        ),
        ENTITY_GROUP_JOIN,
        combine_and_conditions(
            [
                binary_condition(
                    ConditionFunctions.EQ,
                    Column(None, "ev", "project_id"),
                    Literal(None, 1),
                ),
                binary_condition(
                    ConditionFunctions.EQ,
                    Column(None, "gr", "project_id"),
                    Literal(None, 1),
                ),
            ],
        ),
        id="Add project_id simple condition",
    ),
    pytest.param(
        binary_condition(
            ConditionFunctions.EQ,
            FunctionCall(
                None,
                "some_func",
                (Column(None, "ev", "project_id"), Column(None, "ev", "message")),
            ),
            Literal(None, 1),
        ),
        ENTITY_GROUP_JOIN,
        binary_condition(
            ConditionFunctions.EQ,
            FunctionCall(
                None,
                "some_func",
                (Column(None, "ev", "project_id"), Column(None, "ev", "message")),
            ),
            Literal(None, 1),
        ),
        id="Top level condition containing multiple columns. No addition",
    ),
    pytest.param(
        FunctionCall(
            None,
            BooleanFunctions.OR,
            (
                binary_condition(
                    ConditionFunctions.EQ,
                    Column(None, "ev", "project_id"),
                    Literal(None, 1),
                ),
                binary_condition(
                    ConditionFunctions.EQ,
                    Column(None, "gr", "message"),
                    Literal(None, "asd"),
                ),
            ),
        ),
        ENTITY_GROUP_JOIN,
        FunctionCall(
            None,
            BooleanFunctions.OR,
            (
                binary_condition(
                    ConditionFunctions.EQ,
                    Column(None, "ev", "project_id"),
                    Literal(None, 1),
                ),
                binary_condition(
                    ConditionFunctions.EQ,
                    Column(None, "gr", "message"),
                    Literal(None, "asd"),
                ),
            ),
        ),
        id="Or condition with columns of both tables. No addition",
    ),
    pytest.param(
        binary_condition(
            ConditionFunctions.EQ,
            FunctionCall(None, "func", (Column(None, "ev", "project_id"),)),
            Literal(None, 1),
        ),
        ENTITY_GROUP_JOIN,
        combine_and_conditions(
            [
                binary_condition(
                    ConditionFunctions.EQ,
                    FunctionCall(None, "func", (Column(None, "ev", "project_id"),)),
                    Literal(None, 1),
                ),
                binary_condition(
                    ConditionFunctions.EQ,
                    FunctionCall(None, "func", (Column(None, "gr", "project_id"),)),
                    Literal(None, 1),
                ),
            ],
        ),
        id="Complex condition, one column. Add a new one",
    ),
    pytest.param(
        combine_and_conditions(
            [
                binary_condition(
                    ConditionFunctions.EQ,
                    FunctionCall(None, "func", (Column(None, "ev", "project_id"),)),
                    Literal(None, 1),
                ),
                binary_condition(
                    ConditionFunctions.EQ, Column(None, "gr", "id"), Literal(None, 1),
                ),
            ]
        ),
        ENTITY_GROUP_JOIN,
        combine_and_conditions(
            [
                binary_condition(
                    ConditionFunctions.EQ,
                    FunctionCall(None, "func", (Column(None, "ev", "project_id"),)),
                    Literal(None, 1),
                ),
                binary_condition(
                    ConditionFunctions.EQ, Column(None, "gr", "id"), Literal(None, 1),
                ),
                binary_condition(
                    ConditionFunctions.EQ,
                    FunctionCall(None, "func", (Column(None, "gr", "project_id"),)),
                    Literal(None, 1),
                ),
                binary_condition(
                    ConditionFunctions.EQ,
                    Column(None, "ev", "group_id"),
                    Literal(None, 1),
                ),
            ]
        ),
        id="Multiple valid conditions to add",
    ),
]


@pytest.mark.parametrize(
    "initial_condition, join_clause, expected_expr", TEST_REPLACEMENT
)
def test_add_equivalent_condition(
    initial_condition: Expression,
    join_clause: JoinClause[EntitySource],
    expected_expr: Expression,
) -> None:
    ENTITY_IMPL[EntityKey.EVENTS] = Events()
    ENTITY_IMPL[EntityKey.GROUPEDMESSAGES] = GroupedMessage()

    query = CompositeQuery(
        from_clause=join_clause,
        selected_columns=[
            SelectedExpression(
                "group_id", FunctionCall("something", "f", (Column(None, "gr", "id"),))
            )
        ],
        condition=initial_condition,
    )
    add_equivalent_conditions(query)
    assert query.get_condition_from_ast() == expected_expr

    ENTITY_IMPL.clear()
