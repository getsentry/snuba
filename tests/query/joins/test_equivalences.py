import pytest

from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import override_entity_map, reset_entity_factory
from snuba.query.data_source.join import (
    IndividualNode,
    JoinClause,
    JoinCondition,
    JoinConditionExpression,
    JoinType,
)
from snuba.query.data_source.simple import Entity as EntitySource
from snuba.query.joins.pre_processor import (
    EquivalenceGraph,
    QualifiedCol,
    get_equivalent_columns,
)
from tests.query.joins.equivalence_schema import (
    EVENTS_SCHEMA,
    GROUPS_ASSIGNEE,
    GROUPS_SCHEMA,
    Events,
    GroupAssignee,
    GroupedMessage,
)

pytest.skip(allow_module_level=True, reason="Dataset no longer exists")

TEST_CASES = [
    pytest.param(
        JoinClause(
            IndividualNode("ev", EntitySource(EntityKey.EVENTS, EVENTS_SCHEMA, None)),
            IndividualNode(
                "gr", EntitySource(EntityKey.GROUPEDMESSAGE, GROUPS_SCHEMA, None)
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
                QualifiedCol(EntityKey.GROUPEDMESSAGE, "id"),
            },
            QualifiedCol(EntityKey.GROUPEDMESSAGE, "id"): {
                QualifiedCol(EntityKey.EVENTS, "group_id"),
            },
            QualifiedCol(EntityKey.EVENTS, "project_id"): {
                QualifiedCol(EntityKey.GROUPEDMESSAGE, "project_id"),
            },
            QualifiedCol(EntityKey.GROUPEDMESSAGE, "project_id"): {
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
                "gr", EntitySource(EntityKey.GROUPEDMESSAGE, GROUPS_SCHEMA, None)
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
                QualifiedCol(EntityKey.GROUPEDMESSAGE, "id"),
                QualifiedCol(EntityKey.GROUPASSIGNEE, "group_id"),
            },
            QualifiedCol(EntityKey.GROUPEDMESSAGE, "id"): {
                QualifiedCol(EntityKey.EVENTS, "group_id"),
                QualifiedCol(EntityKey.GROUPASSIGNEE, "group_id"),
            },
            QualifiedCol(EntityKey.GROUPASSIGNEE, "group_id"): {
                QualifiedCol(EntityKey.EVENTS, "group_id"),
                QualifiedCol(EntityKey.GROUPEDMESSAGE, "id"),
            },
            QualifiedCol(EntityKey.EVENTS, "project_id"): {
                QualifiedCol(EntityKey.GROUPEDMESSAGE, "project_id"),
                QualifiedCol(EntityKey.GROUPASSIGNEE, "project_id"),
            },
            QualifiedCol(EntityKey.GROUPEDMESSAGE, "project_id"): {
                QualifiedCol(EntityKey.GROUPASSIGNEE, "project_id"),
                QualifiedCol(EntityKey.EVENTS, "project_id"),
            },
            QualifiedCol(EntityKey.GROUPASSIGNEE, "project_id"): {
                QualifiedCol(EntityKey.GROUPEDMESSAGE, "project_id"),
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
                    "gr", EntitySource(EntityKey.GROUPEDMESSAGE, GROUPS_SCHEMA, None)
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
                QualifiedCol(EntityKey.GROUPEDMESSAGE, "id"),
            },
            QualifiedCol(EntityKey.GROUPEDMESSAGE, "id"): {
                QualifiedCol(EntityKey.EVENTS, "group_id"),
            },
            QualifiedCol(EntityKey.GROUPEDMESSAGE, "user_id"): {
                QualifiedCol(EntityKey.GROUPASSIGNEE, "user_id"),
            },
            QualifiedCol(EntityKey.GROUPASSIGNEE, "user_id"): {
                QualifiedCol(EntityKey.GROUPEDMESSAGE, "user_id"),
            },
            QualifiedCol(EntityKey.EVENTS, "project_id"): {
                QualifiedCol(EntityKey.GROUPEDMESSAGE, "project_id"),
                QualifiedCol(EntityKey.GROUPASSIGNEE, "project_id"),
            },
            QualifiedCol(EntityKey.GROUPEDMESSAGE, "project_id"): {
                QualifiedCol(EntityKey.GROUPASSIGNEE, "project_id"),
                QualifiedCol(EntityKey.EVENTS, "project_id"),
            },
            QualifiedCol(EntityKey.GROUPASSIGNEE, "project_id"): {
                QualifiedCol(EntityKey.GROUPEDMESSAGE, "project_id"),
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
    override_entity_map(EntityKey.EVENTS, Events())
    override_entity_map(EntityKey.GROUPEDMESSAGE, GroupedMessage())
    override_entity_map(EntityKey.GROUPASSIGNEE, GroupAssignee())

    assert get_equivalent_columns(join) == graph

    reset_entity_factory()
