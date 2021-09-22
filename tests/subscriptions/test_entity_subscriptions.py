from typing import Any, List, Mapping, Optional, Type

import pytest

from snuba.query import Column, binary_condition
from snuba.query.conditions import ConditionFunctions
from snuba.query.exceptions import InvalidQueryException
from snuba.query.expressions import FunctionCall, Literal
from snuba.subscriptions.entity_subscription import (
    EntitySubscription,
    EventsSubscription,
    SessionsSubscription,
    SubscriptionType,
    TransactionsSubscription,
)

TESTS = [
    pytest.param(
        EventsSubscription,
        {"subscription_type": SubscriptionType.SNQL, "data_dict": {}},
        None,
        id="Events subscription",
    ),
    pytest.param(
        TransactionsSubscription,
        {"subscription_type": SubscriptionType.SNQL, "data_dict": {}},
        None,
        id="Transactions subscription",
    ),
    pytest.param(
        SessionsSubscription,
        {"subscription_type": SubscriptionType.SNQL, "data_dict": {"organization": 1}},
        None,
        id="Sessions subscription",
    ),
    pytest.param(
        SessionsSubscription,
        {"subscription_type": SubscriptionType.SNQL, "data_dict": {}},
        InvalidQueryException,
        id="Sessions subscription",
    ),
]

TESTS_CONDITIONS_METHOD = [
    pytest.param(
        EventsSubscription(subscription_type=SubscriptionType.SNQL, data_dict={}),
        [
            binary_condition(
                ConditionFunctions.LTE,
                FunctionCall(
                    None, "ifNull", (Column(None, None, "offset"), Literal(None, 0)),
                ),
                Literal(None, 5),
            )
        ],
        True,
        id="Events subscription with offset of type SNQL",
    ),
    pytest.param(
        EventsSubscription(subscription_type=SubscriptionType.LEGACY, data_dict={}),
        [[["ifnull", ["offset", 0]], "<=", 5]],
        True,
        id="Events subscription with offset of type LEGACY",
    ),
    pytest.param(
        EventsSubscription(subscription_type=SubscriptionType.LEGACY, data_dict={}),
        [],
        False,
        id="Events subscription with no offset of type LEGACY",
    ),
    pytest.param(
        EventsSubscription(subscription_type=SubscriptionType.SNQL, data_dict={}),
        [],
        False,
        id="Events subscription with no offset of type SNQL",
    ),
    pytest.param(
        TransactionsSubscription(subscription_type=SubscriptionType.SNQL, data_dict={}),
        [
            binary_condition(
                ConditionFunctions.LTE,
                FunctionCall(
                    None, "ifNull", (Column(None, None, "offset"), Literal(None, 0)),
                ),
                Literal(None, 5),
            )
        ],
        True,
        id="Transactions subscription with offset of type SNQL",
    ),
    pytest.param(
        TransactionsSubscription(
            subscription_type=SubscriptionType.LEGACY, data_dict={}
        ),
        [[["ifnull", ["offset", 0]], "<=", 5]],
        True,
        id="Transactions subscription with offset of type LEGACY",
    ),
    pytest.param(
        TransactionsSubscription(
            subscription_type=SubscriptionType.LEGACY, data_dict={}
        ),
        [],
        False,
        id="Transactions subscription with no offset of type LEGACY",
    ),
    pytest.param(
        TransactionsSubscription(subscription_type=SubscriptionType.SNQL, data_dict={}),
        [],
        False,
        id="Transactions subscription with no offset of type SNQL",
    ),
    pytest.param(
        SessionsSubscription(
            subscription_type=SubscriptionType.SNQL, data_dict={"organization": 1}
        ),
        [
            binary_condition(
                ConditionFunctions.EQ, Column(None, None, "org_id"), Literal(None, 1),
            ),
        ],
        True,
        id="Sessions subscription of type SNQL",
    ),
    pytest.param(
        SessionsSubscription(
            subscription_type=SubscriptionType.LEGACY, data_dict={"organization": 1}
        ),
        [],
        True,
        id="Sessions subscription of type LEGACY",
    ),
]


@pytest.mark.parametrize("entity_subscription, creation_dict, exception", TESTS)
def test_basic(
    entity_subscription: Type[EntitySubscription],
    creation_dict: Mapping[str, any],
    exception: Optional[Exception],
) -> None:
    if exception is not None:
        with pytest.raises(exception):
            entity_subscription(**creation_dict)
    else:
        es = entity_subscription(**creation_dict)
        assert es.to_dict() == creation_dict["data_dict"]


@pytest.mark.parametrize(
    "entity_subscription, expected_conditions, has_offset", TESTS_CONDITIONS_METHOD
)
def test_sessions_subscription_functions(
    entity_subscription: EntitySubscription,
    expected_conditions: List[Any],
    has_offset: bool,
) -> None:
    offset = 5 if has_offset else None
    assert (
        entity_subscription.get_entity_subscription_conditions(offset)
        == expected_conditions
    )
