import pytest
import uuid
from typing import Sequence, Tuple, Union

from snuba.datasets.entities import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.query.data_source.join import (
    IndividualNode,
    JoinClause,
    JoinClass,
    JoinCondition,
    JoinConditionExpression,
    JoinRelationship,
    JoinType,
)
from snuba.query.data_source.simple import Entity as QueryEntity
from snuba.query.snql.joins import build_join_clause, RelationshipTuple

###################################
## Used to build expected result ##
###################################


def node(alias: str, name: str) -> IndividualNode[QueryEntity]:
    return IndividualNode(
        alias,
        QueryEntity(EntityKey(name), get_entity(EntityKey(name)).get_data_model()),
    )


def join_clause(
    lhs_alias: str, lhs: Union[str, JoinClause[QueryEntity]], rhs: str
) -> JoinClause[QueryEntity]:
    rhs_alias, rhs = rhs.split(":", 1)
    return JoinClause(
        left_node=node(lhs_alias, lhs) if isinstance(lhs, str) else lhs,
        right_node=node(rhs_alias, rhs),
        keys=[
            JoinCondition(
                left=JoinConditionExpression(lhs_alias, "event_id"),
                right=JoinConditionExpression(rhs_alias, "event_id"),
            )
        ],
        join_type=JoinType.INNER,
    )


test_cases = [
    pytest.param(
        [("ev:events", "t:transactions")],
        join_clause("ev", "events", "t:transactions"),
        id="simple",
    ),
    pytest.param(
        [("ev:events", "t:transactions"), ("t:transactions", "er:errors")],
        join_clause("t", join_clause("ev", "events", "t:transactions"), "er:errors"),
        id="depth=2",
    ),
    pytest.param(
        [("t:transactions", "er:errors"), ("ev:events", "t:transactions")],
        join_clause("t", join_clause("ev", "events", "t:transactions"), "er:errors"),
        id="depth=2 bottom-up",
    ),
    pytest.param(
        [
            ("ev:events", "t:transactions"),
            ("t:transactions", "er:errors"),
            ("er:errors", "or:outcomes_raw"),
        ],
        join_clause(
            "er",
            join_clause(
                "t", join_clause("ev", "events", "t:transactions"), "er:errors"
            ),
            "or:outcomes_raw",
        ),
        id="depth=3",
    ),
    pytest.param(
        [
            ("er:errors", "or:outcomes_raw"),
            ("t:transactions", "er:errors"),
            ("ev:events", "t:transactions"),
        ],
        join_clause(
            "er",
            join_clause(
                "t", join_clause("ev", "events", "t:transactions"), "er:errors"
            ),
            "or:outcomes_raw",
        ),
        id="depth=3 bottom-up",
    ),
    pytest.param(
        [
            ("er:errors", "or:outcomes_raw"),
            ("ev:events", "t:transactions"),
            ("t:transactions", "er:errors"),
        ],
        join_clause(
            "er",
            join_clause(
                "t", join_clause("ev", "events", "t:transactions"), "er:errors"
            ),
            "or:outcomes_raw",
        ),
        id="depth=3 orphan",
    ),
    pytest.param(
        [("ev:events", "t:transactions"), ("ev:events", "er:errors")],
        join_clause("ev", join_clause("ev", "events", "er:errors"), "t:transactions"),
        id="breadth=2",
    ),
    pytest.param(
        [
            ("ev:events", "t:transactions"),
            ("ev:events", "er:errors"),
            ("ev:events", "or:outcomes_raw"),
        ],
        join_clause(
            "ev",
            join_clause(
                "ev", join_clause("ev", "events", "or:outcomes_raw"), "er:errors"
            ),
            "t:transactions",
        ),
        id="breadth=3",
    ),
    pytest.param(
        [
            ("ev:events", "t:transactions"),
            ("t:transactions", "er:errors"),
            ("ev:events", "or:outcomes_raw"),
            ("or:outcomes_raw", "de:discover_events"),
        ],
        join_clause(
            "t",
            join_clause(
                "ev",
                join_clause(
                    "or",
                    join_clause("ev", "events", "or:outcomes_raw"),
                    "de:discover_events",
                ),
                "t:transactions",
            ),
            "er:errors",
        ),
        id="depth=2 breadth=2",
    ),
    pytest.param(
        [
            ("t:transactions", "er:errors"),
            ("or:outcomes_raw", "de:discover_events"),
            ("ev:events", "t:transactions"),
            ("ev:events", "or:outcomes_raw"),
        ],
        join_clause(
            "t",
            join_clause(
                "ev",
                join_clause(
                    "or",
                    join_clause("ev", "events", "or:outcomes_raw"),
                    "de:discover_events",
                ),
                "t:transactions",
            ),
            "er:errors",
        ),
        id="depth=2 breadth=2",
    ),
    pytest.param(
        [
            ("ev:events", "t:transactions"),
            ("t:transactions", "er:errors"),
            ("er:errors", "ev:events"),
        ],
        join_clause(
            "er",
            join_clause(
                "t", join_clause("ev", "events", "t:transactions"), "er:errors"
            ),
            "ev:events",
        ),
        id="depth=3 cycle",
    ),
    pytest.param(
        [
            ("er:errors", "ev:events"),
            ("t:transactions", "er:errors"),
            ("ev:events", "t:transactions"),
        ],
        join_clause(
            "ev",
            join_clause(
                "er", join_clause("t", "transactions", "er:errors"), "ev:events"
            ),
            "t:transactions",
        ),
        id="depth=3 cycle",
    ),
]


@pytest.mark.parametrize("clauses, expected", test_cases)
def test_joins(
    clauses: Sequence[Tuple[str, str]], expected: JoinClause[QueryEntity]
) -> None:
    relationships = []

    for clause in clauses:
        lhs, rhs = clause
        lhs_alias, lhs = lhs.split(":", 1)
        rhs_alias, rhs = rhs.split(":", 1)
        data = JoinRelationship(
            rhs_entity=EntityKey(rhs),
            join_class=JoinClass.ONE_2_ONE,
            join_type=JoinType.INNER,
            columns=[("event_id", "event_id")],
        )
        relationships.append(
            RelationshipTuple(
                node(lhs_alias, lhs), uuid.uuid4().hex, node(rhs_alias, rhs), data,
            )
        )

    result = build_join_clause(relationships)
    assert result == expected
