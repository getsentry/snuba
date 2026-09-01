import pytest

from snuba.clickhouse.columns import UUID, ColumnSet, UInt
from snuba.datasets.entities.entity_key import EntityKey
from snuba.query import SelectedExpression
from snuba.query.composite import CompositeQuery
from snuba.query.conditions import BooleanFunctions, ConditionFunctions, binary_condition
from snuba.query.data_source.projects_finder import ProjectsFinder
from snuba.query.data_source.simple import Entity, LogicalDataSource
from snuba.query.expressions import Column, FunctionCall, Literal
from snuba.query.logical import Query
from snuba.utils.schemas import Column as EntityColumn

EVENTS_SCHEMA = ColumnSet(
    [
        EntityColumn("event_id", UUID()),
        EntityColumn("project_id", UInt(32)),
        EntityColumn("group_id", UInt(32)),
    ]
)


SIMPLE_QUERY = Query(
    Entity(EntityKey.EVENTS, EVENTS_SCHEMA),
    selected_columns=[
        SelectedExpression(
            "alias",
            Column("_snuba_project", None, "project_id"),
        )
    ],
    array_join=None,
    condition=binary_condition(
        ConditionFunctions.IN,
        Column("_snuba_project", None, "project_id"),
        FunctionCall(None, "tuple", (Literal(None, 1), Literal(None, 2))),
    ),
)

AND_OF_OR_QUERY = Query(
    Entity(EntityKey.EVENTS, EVENTS_SCHEMA),
    selected_columns=[
        SelectedExpression(
            "alias",
            Column("_snuba_project", None, "project_id"),
        )
    ],
    array_join=None,
    condition=binary_condition(
        BooleanFunctions.AND,
        binary_condition(
            BooleanFunctions.OR,
            binary_condition(
                ConditionFunctions.EQ,
                Column("_snuba_project", None, "project_id"),
                Literal(None, 1),
            ),
            binary_condition(
                ConditionFunctions.EQ,
                Column("_snuba_project", None, "project_id"),
                Literal(None, 42069),
            ),
        ),
        binary_condition(
            BooleanFunctions.OR,
            binary_condition(
                ConditionFunctions.EQ,
                Column("_snuba_project", None, "project_id"),
                Literal(None, 1),
            ),
            binary_condition(
                ConditionFunctions.EQ,
                Column(None, None, "platform"),
                Literal(None, "x"),
            ),
        ),
    ),
)

TEST_CASES = [
    pytest.param(
        SIMPLE_QUERY,
        {1, 2},
        id="Simple Query",
    ),
    pytest.param(
        CompositeQuery(
            from_clause=SIMPLE_QUERY,
            selected_columns=[
                SelectedExpression(
                    "alias",
                    FunctionCall("alias", "something", (Column(None, None, "alias"),)),
                )
            ],
        ),
        {1, 2},
        id="Nested query. Project from the inner query",
    ),
    pytest.param(
        AND_OF_OR_QUERY,
        {1, 42069},
        id="AND of ORs unions mentioned project ids",
    ),
]


@pytest.mark.parametrize(
    "query, expected_proj",
    TEST_CASES,
)
def test_count_columns(
    query: Query | CompositeQuery[LogicalDataSource],
    expected_proj: set[int],
) -> None:
    project_finder = ProjectsFinder()
    assert project_finder.visit(query) == expected_proj
