from typing import Any, List, Optional

import pytest

from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.query.conditions import ConditionFunctions, binary_condition
from snuba.query.expressions import Column, Literal

TESTS_CONDITIONS_SNQL_METHOD = [
    pytest.param(
        EntityKey.EVENTS,
        None,
        [],
        True,
        id="Events subscription with offset of type SNQL",
    ),
    pytest.param(
        EntityKey.EVENTS,
        None,
        [],
        False,
        id="Events subscription with no offset of type SNQL",
    ),
    pytest.param(
        EntityKey.TRANSACTIONS,
        None,
        [],
        True,
        id="Transactions subscription with offset of type SNQL",
    ),
    pytest.param(
        EntityKey.TRANSACTIONS,
        None,
        [],
        False,
        id="Transactions subscription with no offset of type SNQL",
    ),
    pytest.param(
        EntityKey.SESSIONS,
        1,
        [
            binary_condition(
                ConditionFunctions.EQ,
                Column(None, None, "org_id"),
                Literal(None, 1),
            ),
        ],
        True,
        id="Sessions subscription of type SNQL",
    ),
    pytest.param(
        EntityKey.METRICS_COUNTERS,
        1,
        [
            binary_condition(
                ConditionFunctions.EQ,
                Column(None, None, "org_id"),
                Literal(None, 1),
            ),
        ],
        True,
        id="Metrics counters subscription of type SNQL",
    ),
    pytest.param(
        EntityKey.METRICS_SETS,
        1,
        [
            binary_condition(
                ConditionFunctions.EQ,
                Column(None, None, "org_id"),
                Literal(None, 1),
            ),
        ],
        True,
        id="Metrics sets subscription of type SNQL",
    ),
]


@pytest.mark.parametrize(
    "entity_key, organization, expected_conditions, has_offset",
    TESTS_CONDITIONS_SNQL_METHOD,
)
def test_entity_subscription_methods_for_snql(
    entity_key: EntityKey,
    organization: Optional[int],
    expected_conditions: List[Any],
    has_offset: bool,
) -> None:
    entity_subscription = get_entity(entity_key).get_entity_subscription()
    assert entity_subscription is not None
    offset = 5 if has_offset else None
    assert (
        entity_subscription.get_entity_subscription_conditions_for_snql(
            offset, organization
        )
        == expected_conditions
    )
