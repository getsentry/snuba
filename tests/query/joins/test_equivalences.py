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
    GROUPS_SCHEMA,
    Events,
    Profiles,
)

TEST_CASES = [
    pytest.param(
        JoinClause(
            IndividualNode("ev", EntitySource(EntityKey.EVENTS, EVENTS_SCHEMA, None)),
            IndividualNode("gr", EntitySource(EntityKey.PROFILES, GROUPS_SCHEMA, None)),
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
                QualifiedCol(EntityKey.PROFILES, "id"),
            },
            QualifiedCol(EntityKey.PROFILES, "id"): {
                QualifiedCol(EntityKey.EVENTS, "group_id"),
            },
            QualifiedCol(EntityKey.EVENTS, "project_id"): {
                QualifiedCol(EntityKey.PROFILES, "project_id"),
            },
            QualifiedCol(EntityKey.PROFILES, "project_id"): {
                QualifiedCol(EntityKey.EVENTS, "project_id"),
            },
        },
        id="Two entities join",
    ),
]


@pytest.mark.parametrize("join, graph", TEST_CASES)
def test_find_equivalences(join: JoinClause[EntitySource], graph: EquivalenceGraph) -> None:
    override_entity_map(EntityKey.EVENTS, Events())
    override_entity_map(EntityKey.PROFILES, Profiles())

    assert get_equivalent_columns(join) == graph

    reset_entity_factory()
