from typing import Any, List, Mapping, Optional, Type

import pytest

from snuba.datasets.entity import BaseEntitySubscription, EntitySubscription
from snuba.query.conditions import ConditionFunctions, binary_condition
from snuba.query.exceptions import InvalidQueryException
from snuba.query.expressions import Column, Literal

TESTS = [
    pytest.param(
        BaseEntitySubscription,
        {"data_dict": {}},
        None,
        id="Events subscription",
    ),
    pytest.param(
        BaseEntitySubscription,
        {"data_dict": {}},
        None,
        id="Transactions subscription",
    ),
    pytest.param(
        BaseEntitySubscription,
        {"data_dict": {"organization": 1}},
        None,
        id="Sessions subscription",
    ),
    pytest.param(
        BaseEntitySubscription,
        {"data_dict": {}},
        InvalidQueryException,
        id="Sessions subscription",
    ),
    pytest.param(
        BaseEntitySubscription,
        {"data_dict": {"organization": 1}},
        None,
        id="Metrics counters subscription",
    ),
    pytest.param(
        BaseEntitySubscription,
        {"data_dict": {}},
        InvalidQueryException,
        id="Metrics counters subscription",
    ),
    pytest.param(
        BaseEntitySubscription,
        {"data_dict": {"organization": 1}},
        None,
        id="Metrics sets subscription",
    ),
    pytest.param(
        BaseEntitySubscription,
        {"data_dict": {}},
        InvalidQueryException,
        id="Metrics sets subscription",
    ),
]

TESTS_CONDITIONS_SNQL_METHOD = [
    pytest.param(
        BaseEntitySubscription(data_dict={}),
        [],
        True,
        id="Events subscription with offset of type SNQL",
    ),
    pytest.param(
        BaseEntitySubscription(data_dict={}),
        [],
        False,
        id="Events subscription with no offset of type SNQL",
    ),
    pytest.param(
        BaseEntitySubscription(data_dict={}),
        [],
        True,
        id="Transactions subscription with offset of type SNQL",
    ),
    pytest.param(
        BaseEntitySubscription(data_dict={}),
        [],
        False,
        id="Transactions subscription with no offset of type SNQL",
    ),
    pytest.param(
        BaseEntitySubscription(data_dict={"organization": 1}),
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
        BaseEntitySubscription(data_dict={"organization": 1}),
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
        BaseEntitySubscription(data_dict={"organization": 1}),
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


@pytest.mark.parametrize("entity_subscription, creation_dict, exception", TESTS)
def test_basic(
    entity_subscription: Type[EntitySubscription],
    creation_dict: Mapping[str, Any],
    exception: Optional[Type[Exception]],
) -> None:
    if exception is not None:
        with pytest.raises(exception):
            entity_subscription(**creation_dict)
    else:
        es = entity_subscription(**creation_dict)
        assert es.to_dict() == creation_dict["data_dict"]


@pytest.mark.parametrize(
    "entity_subscription, expected_conditions, has_offset", TESTS_CONDITIONS_SNQL_METHOD
)
def test_entity_subscription_methods_for_snql(
    entity_subscription: EntitySubscription,
    expected_conditions: List[Any],
    has_offset: bool,
) -> None:
    offset = 5 if has_offset else None
    assert (
        entity_subscription.get_entity_subscription_conditions_for_snql(offset)
        == expected_conditions
    )
