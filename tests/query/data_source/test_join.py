from snuba.clickhouse.columns import UUID, ColumnSet, String, UInt
from snuba.datasets.entities.entity_key import EntityKey
from snuba.query.data_source.join import (
    IndividualNode,
    JoinClause,
    JoinCondition,
    JoinConditionExpression,
    JoinModifier,
    JoinType,
)
from snuba.query.data_source.simple import Entity

ERRORS_SCHEMA = ColumnSet([("event_id", UUID()), ("message", String()), ("group_id", UInt(32))])

GROUPS_SCHEMA = ColumnSet([("id", UInt(32)), ("message", String())])


def test_entity_node() -> None:
    e = Entity(key=EntityKey.EVENTS, schema=ERRORS_SCHEMA)
    node = IndividualNode(alias="err", data_source=e)

    assert node.get_column_sets() == {"err": e.schema}


def test_simple_join() -> None:
    e = Entity(key=EntityKey.EVENTS, schema=ERRORS_SCHEMA)
    node_err = IndividualNode(alias="err", data_source=e)

    g = Entity(key=EntityKey.PROFILES, schema=GROUPS_SCHEMA)
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
