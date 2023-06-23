import pytest

from snuba.clickhouse.columns import UUID, Any, ColumnSet, String, UInt
from snuba.datasets.entities.entity_key import EntityKey
from snuba.query import SelectedExpression
from snuba.query.data_source.join import (
    IndividualNode,
    JoinClause,
    JoinCondition,
    JoinConditionExpression,
    JoinModifier,
    JoinType,
)
from snuba.query.data_source.simple import Entity
from snuba.query.expressions import Column
from snuba.query.logical import Query

ERRORS_SCHEMA = ColumnSet(
    [("event_id", UUID()), ("message", String()), ("group_id", UInt(32))]
)

GROUPS_SCHEMA = ColumnSet([("id", UInt(32)), ("message", String())])

GROUPS_ASSIGNEE = ColumnSet([("id", UInt(32)), ("user", String())])


def test_entity_node() -> None:
    e = Entity(key=EntityKey.EVENTS, schema=ERRORS_SCHEMA)
    node = IndividualNode(alias="err", data_source=e)

    assert node.get_column_sets() == {"err": e.schema}


@pytest.mark.skip(reason="Dataset no longer exists")
def test_simple_join() -> None:
    e = Entity(key=EntityKey.EVENTS, schema=ERRORS_SCHEMA)
    node_err = IndividualNode(alias="err", data_source=e)

    g = Entity(key=EntityKey.GROUPEDMESSAGE, schema=GROUPS_SCHEMA)
    node_group = IndividualNode(alias="groups", data_source=g)

    join = JoinClause(
        left_node=node_err,
        right_node=node_group,
        keys=[
            JoinCondition(
                left=JoinConditionExpression("err", "group_id"),
                right=JoinConditionExpression("groups", "id"),
            )
        ],
        join_type=JoinType.INNER,
        join_modifier=JoinModifier.SEMI,
    )

    assert join.get_column_sets() == {"err": ERRORS_SCHEMA, "groups": GROUPS_SCHEMA}

    joined_cols = join.get_columns()
    assert "err.group_id" in joined_cols
    assert "err.event_id" in joined_cols
    assert "groups.id" in joined_cols
    assert "groups.message" in joined_cols

    with pytest.raises(AssertionError):
        JoinClause(
            left_node=node_err,
            right_node=node_group,
            keys=[
                JoinCondition(
                    left=JoinConditionExpression("err", "missing_col"),
                    right=JoinConditionExpression("groups", "another_missing_col"),
                )
            ],
            join_type=JoinType.INNER,
        )


@pytest.mark.skip(reason="Dataset no longer exists")
def test_complex_joins() -> None:
    e = Entity(key=EntityKey.EVENTS, schema=ERRORS_SCHEMA)
    node_err = IndividualNode(alias="err", data_source=e)

    g = Entity(key=EntityKey.GROUPEDMESSAGE, schema=GROUPS_SCHEMA)
    node_group = IndividualNode(alias="groups", data_source=g)

    a = Entity(key=EntityKey.GROUPASSIGNEE, schema=GROUPS_ASSIGNEE)
    query = Query(
        from_clause=a,
        selected_columns=[
            SelectedExpression("id", Column("id", None, "id")),
            SelectedExpression("assigned_user", Column("assigned_user", None, "user")),
        ],
    )
    node_query = IndividualNode(alias="assignee", data_source=query)

    join = JoinClause(
        left_node=JoinClause(
            left_node=node_err,
            right_node=node_group,
            keys=[
                JoinCondition(
                    left=JoinConditionExpression("err", "group_id"),
                    right=JoinConditionExpression("groups", "id"),
                )
            ],
            join_type=JoinType.INNER,
        ),
        right_node=node_query,
        keys=[
            JoinCondition(
                left=JoinConditionExpression("err", "group_id"),
                right=JoinConditionExpression("assignee", "id"),
            )
        ],
        join_type=JoinType.INNER,
    )

    assert join.get_column_sets() == {
        "err": ERRORS_SCHEMA,
        "assignee": ColumnSet([("id", Any()), ("assigned_user", Any())]),
        "groups": GROUPS_SCHEMA,
    }
