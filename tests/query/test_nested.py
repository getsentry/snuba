import pytest

from snuba.clickhouse.columns import Any, ColumnSet, String, UInt
from snuba.datasets.entities.entity_key import EntityKey
from snuba.query import SelectedExpression
from snuba.query.composite import CompositeQuery
from snuba.query.data_source.join import (
    IndividualNode,
    JoinClause,
    JoinCondition,
    JoinConditionExpression,
    JoinType,
)
from snuba.query.data_source.simple import Entity
from snuba.query.expressions import Column
from snuba.query.logical import Query as LogicalQuery


def test_nested_query() -> None:
    """
    Simply builds a nested query.
    """

    nested = LogicalQuery(
        Entity(EntityKey.EVENTS, ColumnSet([("event_id", String())])),
        selected_columns=[
            SelectedExpression(
                "string_evt_id", Column("string_evt_id", None, "event_id")
            )
        ],
    )

    composite = CompositeQuery(
        from_clause=nested,
        selected_columns=[
            SelectedExpression("output", Column("output", None, "string_evt_id"))
        ],
    )

    # The iterator methods on the composite query do not descend into
    # the nested query
    assert composite.get_all_ast_referenced_columns() == {
        Column("output", None, "string_evt_id")
    }

    # The schema of the nested query is the selected clause of that query.
    assert composite.get_from_clause().get_columns() == ColumnSet(
        [("string_evt_id", Any())]
    )


@pytest.mark.skip(reason="Dataset no longer exists")
def test_join_query() -> None:
    events_query = LogicalQuery(
        Entity(
            EntityKey.EVENTS,
            ColumnSet([("event_id", String()), ("group_id", UInt(32))]),
        ),
        selected_columns=[
            SelectedExpression("group_id", Column("group_id", None, "group_id")),
            SelectedExpression(
                "string_evt_id", Column("string_evt_id", None, "event_id")
            ),
        ],
    )

    groups_query = LogicalQuery(
        Entity(
            EntityKey.GROUPEDMESSAGE,
            ColumnSet([("id", UInt(32)), ("message", String())]),
        ),
        selected_columns=[
            SelectedExpression("group_id", Column("group_id", None, "id"))
        ],
    )

    join_query = CompositeQuery(
        from_clause=JoinClause(
            left_node=IndividualNode("e", events_query),
            right_node=IndividualNode("g", groups_query),
            keys=[
                JoinCondition(
                    left=JoinConditionExpression("e", "group_id"),
                    right=JoinConditionExpression("g", "group_id"),
                )
            ],
            join_type=JoinType.INNER,
        )
    )

    data_source = join_query.get_from_clause()
    assert "e.string_evt_id" in data_source.get_columns()
    assert "g.group_id" in data_source.get_columns()
