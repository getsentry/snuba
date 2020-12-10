from abc import ABC
from typing import Mapping, Sequence
from unittest.mock import Mock

import pytest
from snuba.clickhouse.columns import UUID, ColumnSet, String, UInt
from snuba.datasets.entities import EntityKey
from snuba.datasets.entities.factory import ENTITY_IMPL
from snuba.datasets.entity import Entity
from snuba.query.data_source.join import (
    ColumnEquivalence,
    IndividualNode,
    JoinClause,
    JoinCondition,
    JoinConditionExpression,
    JoinRelationship,
    JoinType,
)
from snuba.query.data_source.simple import Entity as EntitySource
from snuba.query.extensions import QueryExtension
from snuba.query.joins.pre_processor import (
    EquivalenceGraph,
    QualifiedCol,
    get_equivalent_columns,
)
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
                    columns=[("group_id", "id")],
                    join_type=JoinType.INNER,
                    equivalences=[ColumnEquivalence("project_id", "project_id")],
                ),
                "assigned_group": JoinRelationship(
                    rhs_entity=EntityKey.GROUPASSIGNEE,
                    columns=[("group_id", "group_id")],
                    join_type=JoinType.INNER,
                    equivalences=[ColumnEquivalence("project_id", "project_id")],
                ),
                # This makes no sense but it is for the sake of the test
                "assigned_user": JoinRelationship(
                    rhs_entity=EntityKey.GROUPASSIGNEE,
                    columns=[("user_id", "user_id")],
                    join_type=JoinType.INNER,
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
                    columns=[("id", "group_id")],
                    join_type=JoinType.INNER,
                    equivalences=[ColumnEquivalence("project_id", "project_id")],
                ),
                "assigned": JoinRelationship(
                    rhs_entity=EntityKey.GROUPASSIGNEE,
                    columns=[("user_id", "user_id")],
                    join_type=JoinType.INNER,
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
                    columns=[("group_id", "group_id")],
                    join_type=JoinType.INNER,
                    equivalences=[ColumnEquivalence("project_id", "project_id")],
                ),
                "user_assigned": JoinRelationship(
                    rhs_entity=EntityKey.EVENTS,
                    columns=[("user_id", "user_id")],
                    join_type=JoinType.INNER,
                    equivalences=[ColumnEquivalence("project_id", "project_id")],
                ),
            },
            writable_storage=None,
        )


TEST_CASES = [
    pytest.param(
        JoinClause(
            IndividualNode("ev", EntitySource(EntityKey.EVENTS, EVENTS_SCHEMA, None)),
            IndividualNode(
                "gr", EntitySource(EntityKey.GROUPEDMESSAGES, GROUPS_SCHEMA, None)
            ),
            [
                JoinCondition(
                    JoinConditionExpression("ev", "group_id"),
                    JoinConditionExpression("gr", "id"),
                )
            ],
            JoinType.INNER,
            None,
        ),
        {
            QualifiedCol(EntityKey.EVENTS, "group_id"): {
                QualifiedCol(EntityKey.GROUPEDMESSAGES, "id"),
            },
            QualifiedCol(EntityKey.GROUPEDMESSAGES, "id"): {
                QualifiedCol(EntityKey.EVENTS, "group_id"),
            },
            QualifiedCol(EntityKey.EVENTS, "project_id"): {
                QualifiedCol(EntityKey.GROUPEDMESSAGES, "project_id"),
            },
            QualifiedCol(EntityKey.GROUPEDMESSAGES, "project_id"): {
                QualifiedCol(EntityKey.EVENTS, "project_id"),
            },
        },
        id="Two entities join",
    ),
    pytest.param(
        JoinClause(
            JoinClause(
                IndividualNode(
                    "ev", EntitySource(EntityKey.EVENTS, EVENTS_SCHEMA, None)
                ),
                IndividualNode(
                    "as", EntitySource(EntityKey.GROUPASSIGNEE, GROUPS_ASSIGNEE, None)
                ),
                [
                    JoinCondition(
                        JoinConditionExpression("ev", "group_id"),
                        JoinConditionExpression("as", "group_id"),
                    )
                ],
                JoinType.INNER,
                None,
            ),
            IndividualNode(
                "gr", EntitySource(EntityKey.GROUPEDMESSAGES, GROUPS_SCHEMA, None)
            ),
            [
                JoinCondition(
                    JoinConditionExpression("ev", "group_id"),
                    JoinConditionExpression("gr", "id"),
                )
            ],
            JoinType.INNER,
            None,
        ),
        {
            QualifiedCol(EntityKey.EVENTS, "group_id"): {
                QualifiedCol(EntityKey.GROUPEDMESSAGES, "id"),
                QualifiedCol(EntityKey.GROUPASSIGNEE, "group_id"),
            },
            QualifiedCol(EntityKey.GROUPEDMESSAGES, "id"): {
                QualifiedCol(EntityKey.EVENTS, "group_id"),
                QualifiedCol(EntityKey.GROUPASSIGNEE, "group_id"),
            },
            QualifiedCol(EntityKey.GROUPASSIGNEE, "group_id"): {
                QualifiedCol(EntityKey.EVENTS, "group_id"),
                QualifiedCol(EntityKey.GROUPEDMESSAGES, "id"),
            },
            QualifiedCol(EntityKey.EVENTS, "project_id"): {
                QualifiedCol(EntityKey.GROUPEDMESSAGES, "project_id"),
                QualifiedCol(EntityKey.GROUPASSIGNEE, "project_id"),
            },
            QualifiedCol(EntityKey.GROUPEDMESSAGES, "project_id"): {
                QualifiedCol(EntityKey.GROUPASSIGNEE, "project_id"),
                QualifiedCol(EntityKey.EVENTS, "project_id"),
            },
            QualifiedCol(EntityKey.GROUPASSIGNEE, "project_id"): {
                QualifiedCol(EntityKey.GROUPEDMESSAGES, "project_id"),
                QualifiedCol(EntityKey.EVENTS, "project_id"),
            },
        },
        id="Join with three tables",
    ),
    pytest.param(
        JoinClause(
            JoinClause(
                IndividualNode(
                    "ev", EntitySource(EntityKey.EVENTS, EVENTS_SCHEMA, None)
                ),
                IndividualNode(
                    "gr", EntitySource(EntityKey.GROUPEDMESSAGES, GROUPS_SCHEMA, None)
                ),
                [
                    JoinCondition(
                        JoinConditionExpression("ev", "group_id"),
                        JoinConditionExpression("gr", "id"),
                    )
                ],
                JoinType.INNER,
                None,
            ),
            IndividualNode(
                "as", EntitySource(EntityKey.GROUPASSIGNEE, GROUPS_ASSIGNEE, None)
            ),
            [
                JoinCondition(
                    JoinConditionExpression("gr", "user_id"),
                    JoinConditionExpression("as", "user_id"),
                )
            ],
            JoinType.INNER,
            None,
        ),
        {
            QualifiedCol(EntityKey.EVENTS, "group_id"): {
                QualifiedCol(EntityKey.GROUPEDMESSAGES, "id"),
            },
            QualifiedCol(EntityKey.GROUPEDMESSAGES, "id"): {
                QualifiedCol(EntityKey.EVENTS, "group_id"),
            },
            QualifiedCol(EntityKey.GROUPEDMESSAGES, "user_id"): {
                QualifiedCol(EntityKey.GROUPASSIGNEE, "user_id"),
            },
            QualifiedCol(EntityKey.GROUPASSIGNEE, "user_id"): {
                QualifiedCol(EntityKey.GROUPEDMESSAGES, "user_id"),
            },
            QualifiedCol(EntityKey.EVENTS, "project_id"): {
                QualifiedCol(EntityKey.GROUPEDMESSAGES, "project_id"),
                QualifiedCol(EntityKey.GROUPASSIGNEE, "project_id"),
            },
            QualifiedCol(EntityKey.GROUPEDMESSAGES, "project_id"): {
                QualifiedCol(EntityKey.GROUPASSIGNEE, "project_id"),
                QualifiedCol(EntityKey.EVENTS, "project_id"),
            },
            QualifiedCol(EntityKey.GROUPASSIGNEE, "project_id"): {
                QualifiedCol(EntityKey.GROUPEDMESSAGES, "project_id"),
                QualifiedCol(EntityKey.EVENTS, "project_id"),
            },
        },
        id="Join with three tables",
    ),
]


@pytest.mark.parametrize("join, graph", TEST_CASES)
def test_find_equivalences(
    join: JoinClause[EntitySource], graph: EquivalenceGraph
) -> None:
    ENTITY_IMPL[EntityKey.EVENTS] = Events()
    ENTITY_IMPL[EntityKey.GROUPEDMESSAGES] = GroupedMessage()
    ENTITY_IMPL[EntityKey.GROUPASSIGNEE] = GroupAssignee()

    assert get_equivalent_columns(join) == graph

    ENTITY_IMPL.clear()
