from abc import ABC
from typing import Mapping, Sequence
from unittest.mock import Mock

from snuba.clickhouse.columns import UUID, ColumnSet, String, UInt
from snuba.datasets.entities import EntityKey
from snuba.datasets.entity import ColumnEquivalence, Entity, JoinRelationship
from snuba.query.data_source.join import (
    JoinClass,
    JoinCondition,
    JoinConditionExpression,
)
from snuba.query.extensions import QueryExtension
from snuba.query.processors import QueryProcessor

EVENTS_SCHEMA = ColumnSet(
    [
        ("event_id", UUID()),
        ("project_id", UInt(32)),
        ("message", String()),
        ("group_id", UInt(32)),
        ("user_id", UInt(64)),
    ]
)

GROUPS_SCHEMA = ColumnSet(
    [
        ("id", UUID()),
        ("project_id", UInt(32)),
        ("message", String()),
        ("user_id", UInt(64)),
    ]
)

GROUPS_ASSIGNEE = ColumnSet(
    [
        ("group_id", UUID()),
        ("project_id", UInt(32)),
        ("message", String()),
        ("user_id", UInt(64)),
    ]
)


class FakeEntity(Entity, ABC):
    def get_extensions(self) -> Mapping[str, QueryExtension]:
        return {}

    def get_query_processors(self) -> Sequence[QueryProcessor]:
        return []


class Events(FakeEntity):
    def __init__(self) -> None:
        super().__init__(
            storages=[],
            query_pipeline_builder=Mock(),
            abstract_column_set=EVENTS_SCHEMA,
            join_relationships={
                "grouped": JoinRelationship(
                    rhs_entity=EntityKey.GROUPEDMESSAGES,
                    keys=[
                        JoinCondition(
                            JoinConditionExpression("", "group_id"),
                            JoinConditionExpression("", "id"),
                        )
                    ],
                    join_class=JoinClass.N_2_ONE,
                    equivalences=[ColumnEquivalence("project_id", "project_id")],
                ),
                "assigned_group": JoinRelationship(
                    rhs_entity=EntityKey.GROUPASSIGNEE,
                    keys=[
                        JoinCondition(
                            JoinConditionExpression("", "group_id"),
                            JoinConditionExpression("", "group_id"),
                        )
                    ],
                    join_class=JoinClass.N_2_ONE,
                    equivalences=[ColumnEquivalence("project_id", "project_id")],
                ),
                # This makes no sense but it is for the sake of the test
                "assigned_user": JoinRelationship(
                    rhs_entity=EntityKey.GROUPASSIGNEE,
                    keys=[
                        JoinCondition(
                            JoinConditionExpression("", "user_id"),
                            JoinConditionExpression("", "user_id"),
                        )
                    ],
                    join_class=JoinClass.N_2_ONE,
                    equivalences=[ColumnEquivalence("project_id", "project_id")],
                ),
            },
            writable_storage=None,
        )


class GroupedMessage(FakeEntity):
    def __init__(self) -> None:
        super().__init__(
            storages=[],
            query_pipeline_builder=Mock(),
            abstract_column_set=GROUPS_SCHEMA,
            join_relationships={
                "events": JoinRelationship(
                    rhs_entity=EntityKey.EVENTS,
                    keys=[
                        JoinCondition(
                            JoinConditionExpression("", "id"),
                            JoinConditionExpression("", "group_id"),
                        )
                    ],
                    join_class=JoinClass.ONE_2_N,
                    equivalences=[ColumnEquivalence("project_id", "project_id")],
                ),
                "assigned": JoinRelationship(
                    rhs_entity=EntityKey.GROUPASSIGNEE,
                    keys=[
                        JoinCondition(
                            JoinConditionExpression("", "user_id"),
                            JoinConditionExpression("", "user_id"),
                        )
                    ],
                    join_class=JoinClass.ONE_2_N,
                    equivalences=[],
                ),
            },
            writable_storage=None,
        )


class GroupAssignee(FakeEntity):
    def __init__(self) -> None:
        super().__init__(
            storages=[],
            query_pipeline_builder=Mock(),
            abstract_column_set=GROUPS_ASSIGNEE,
            join_relationships={
                "events": JoinRelationship(
                    rhs_entity=EntityKey.EVENTS,
                    keys=[
                        JoinCondition(
                            JoinConditionExpression("", "group_id"),
                            JoinConditionExpression("", "group_id"),
                        )
                    ],
                    join_class=JoinClass.ONE_2_N,
                    equivalences=[ColumnEquivalence("project_id", "project_id")],
                ),
                "user_assigned": JoinRelationship(
                    rhs_entity=EntityKey.EVENTS,
                    keys=[
                        JoinCondition(
                            JoinConditionExpression("", "user_id"),
                            JoinConditionExpression("", "user_id"),
                        )
                    ],
                    join_class=JoinClass.ONE_2_N,
                    equivalences=[ColumnEquivalence("project_id", "project_id")],
                ),
            },
            writable_storage=None,
        )
