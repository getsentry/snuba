from typing import Any, List, Optional, Type

import pytest

from snuba.datasets.entity_subscriptions.entity_subscription import (
    EntitySubscription,
    EventsSubscription,
    MetricsCountersSubscription,
    MetricsSetsSubscription,
    SessionsSubscription,
    TransactionsSubscription,
)
from snuba.query.conditions import ConditionFunctions, binary_condition
from snuba.query.exceptions import InvalidQueryException
from snuba.query.expressions import Column, Literal

TESTS = [
    pytest.param(
        EventsSubscription,
        None,
        None,
        id="Events subscription",
    ),
    pytest.param(
        TransactionsSubscription,
        None,
        None,
        id="Transactions subscription",
    ),
    pytest.param(
        SessionsSubscription,
        1,
        None,
        id="Sessions subscription",
    ),
    pytest.param(
        SessionsSubscription,
        None,
        InvalidQueryException,
        id="Sessions subscription",
    ),
    pytest.param(
        MetricsCountersSubscription,
        1,
        None,
        id="Metrics counters subscription",
    ),
    pytest.param(
        MetricsCountersSubscription,
        None,
        InvalidQueryException,
        id="Metrics counters subscription",
    ),
    pytest.param(
        MetricsSetsSubscription,
        1,
        None,
        id="Metrics sets subscription",
    ),
    pytest.param(
        MetricsSetsSubscription,
        None,
        InvalidQueryException,
        id="Metrics sets subscription",
    ),
]

TESTS_CONDITIONS_SNQL_METHOD = [
    pytest.param(
        EventsSubscription(),
        [],
        True,
        None,
        id="Events subscription with offset of type SNQL",
    ),
    pytest.param(
        EventsSubscription(),
        [],
        False,
        None,
        id="Events subscription with no offset of type SNQL",
    ),
    pytest.param(
        TransactionsSubscription(),
        [],
        True,
        None,
        id="Transactions subscription with offset of type SNQL",
    ),
    pytest.param(
        TransactionsSubscription(),
        [],
        False,
        None,
        id="Transactions subscription with no offset of type SNQL",
    ),
    pytest.param(
        SessionsSubscription(),
        [
            binary_condition(
                ConditionFunctions.EQ,
                Column(None, None, "org_id"),
                Literal(None, 1),
            ),
        ],
        True,
        1,
        id="Sessions subscription of type SNQL",
    ),
    pytest.param(
        MetricsCountersSubscription(),
        [
            binary_condition(
                ConditionFunctions.EQ,
                Column(None, None, "org_id"),
                Literal(None, 1),
            ),
        ],
        True,
        1,
        id="Metrics counters subscription of type SNQL",
    ),
    pytest.param(
        MetricsSetsSubscription(),
        [
            binary_condition(
                ConditionFunctions.EQ,
                Column(None, None, "org_id"),
                Literal(None, 1),
            ),
        ],
        True,
        1,
        id="Metrics sets subscription of type SNQL",
    ),
]


@pytest.mark.parametrize("entity_subscription, organization, exception", TESTS)
def test_basic(
    entity_subscription: Type[EntitySubscription],
    organization: Optional[int],
    exception: Optional[Type[Exception]],
) -> None:
    es = entity_subscription()
    if exception is not None:
        with pytest.raises(exception):
            es.get_entity_subscription_conditions_for_snql(None, organization)


@pytest.mark.parametrize(
    "entity_subscription, expected_conditions, has_offset, organization",
    TESTS_CONDITIONS_SNQL_METHOD,
)
def test_entity_subscription_methods_for_snql(
    entity_subscription: EntitySubscription,
    expected_conditions: List[Any],
    has_offset: bool,
    organization: Optional[int],
) -> None:
    offset = 5 if has_offset else None
    assert (
        entity_subscription.get_entity_subscription_conditions_for_snql(
            offset, organization
        )
        == expected_conditions
    )
