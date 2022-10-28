from typing import Any, List, Mapping, Optional, Type

import pytest

from snuba.datasets.entity_subscriptions.entity_subscription import EntitySubscription
from snuba.query.conditions import ConditionFunctions, binary_condition
from snuba.query.exceptions import InvalidQueryException
from snuba.query.expressions import Column, Literal

TESTS = [
    pytest.param(
        EntitySubscription,
        {"has_org_id": False},
        None,
        None,
        id="Events subscription",
    ),
    pytest.param(
        EntitySubscription,
        {"has_org_id": False},
        None,
        None,
        id="Transactions subscription",
    ),
    pytest.param(
        EntitySubscription,
        {"has_org_id": True},
        1,
        None,
        id="Sessions subscription",
    ),
    pytest.param(
        EntitySubscription,
        {"has_org_id": True},
        None,
        InvalidQueryException,
        id="Sessions subscription",
    ),
    pytest.param(
        EntitySubscription,
        {"has_org_id": True},
        1,
        None,
        id="Metrics counters subscription",
    ),
    pytest.param(
        EntitySubscription,
        {"has_org_id": True},
        None,
        InvalidQueryException,
        id="Metrics counters subscription",
    ),
    pytest.param(
        EntitySubscription,
        {"has_org_id": True},
        1,
        None,
        id="Metrics sets subscription",
    ),
    pytest.param(
        EntitySubscription,
        {"has_org_id": True},
        None,
        InvalidQueryException,
        id="Metrics sets subscription",
    ),
]

TESTS_CONDITIONS_SNQL_METHOD = [
    pytest.param(
        EntitySubscription(has_org_id=False),
        None,
        [],
        True,
        id="Events subscription with offset of type SNQL",
    ),
    pytest.param(
        EntitySubscription(has_org_id=False),
        None,
        [],
        False,
        id="Events subscription with no offset of type SNQL",
    ),
    pytest.param(
        EntitySubscription(has_org_id=False),
        None,
        [],
        True,
        id="Transactions subscription with offset of type SNQL",
    ),
    pytest.param(
        EntitySubscription(has_org_id=False),
        None,
        [],
        False,
        id="Transactions subscription with no offset of type SNQL",
    ),
    pytest.param(
        EntitySubscription(has_org_id=True),
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
        EntitySubscription(has_org_id=True),
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
        EntitySubscription(has_org_id=True),
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


@pytest.mark.parametrize("entity_subscription, creation_dict, org_id, exception", TESTS)
def test_basic(
    entity_subscription: Type[EntitySubscription],
    org_id: Optional[int],
    creation_dict: Mapping[str, Any],
    exception: Optional[Type[Exception]],
) -> None:
    es = entity_subscription(**creation_dict)
    if exception is not None:
        with pytest.raises(exception):
            if es.has_org_id and not org_id:
                raise InvalidQueryException("organization param is required")
    else:
        if es.has_org_id:
            assert org_id == 1
        else:
            assert org_id is None


@pytest.mark.parametrize(
    "entity_subscription, org_id, expected_conditions, has_offset",
    TESTS_CONDITIONS_SNQL_METHOD,
)
def test_entity_subscription_methods_for_snql(
    entity_subscription: EntitySubscription,
    org_id: Optional[int],
    expected_conditions: List[Any],
    has_offset: bool,
) -> None:
    offset = 5 if has_offset else None
    assert (
        entity_subscription.get_entity_subscription_conditions_for_snql(offset, org_id)
        == expected_conditions
    )
