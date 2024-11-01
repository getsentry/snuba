from typing import Set, Union

import pytest

from snuba.clickhouse.columns import UUID, ColumnSet, UInt
from snuba.datasets.entities.entity_key import EntityKey
from snuba.query import SelectedExpression
from snuba.query.composite import CompositeQuery
from snuba.query.conditions import ConditionFunctions, binary_condition
from snuba.query.data_source.projects_finder import ProjectsFinder
from snuba.query.data_source.simple import Entity
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
]


@pytest.mark.parametrize(
    "query, expected_proj",
    TEST_CASES,
)
def test_count_columns(
    query: Union[Query, CompositeQuery[Entity]],
    expected_proj: Set[int],
) -> None:
    project_finder = ProjectsFinder()
    assert project_finder.visit(query) == expected_proj  # type: ignore
