from typing import Type, Union

import pytest
from jsonschema.exceptions import ValidationError

from snuba.datasets.configuration.entity_subscription_builder import (
    build_entity_subscription_from_config,
)
from snuba.datasets.entity_subscriptions.entity_subscription import (
    ComplexEntitySubscription,
    GenericMetricsSetsSubscription,
    SimpleEntitySubscription,
    TransactionsSubscription,
)
from snuba.datasets.entity_subscriptions.pluggable_entity_subscription import (
    PluggableEntitySubscriptionComplex,
    PluggableEntitySubscriptionSimple,
)

data = {"organization": 1}


def test_build_complex_entity_subscription_from_config() -> None:
    config_sets_entity_subscription: Type[
        Union[PluggableEntitySubscriptionComplex, PluggableEntitySubscriptionSimple]
    ] = build_entity_subscription_from_config(
        "snuba/datasets/configuration/generic_metrics/entity_subscriptions/sets.yaml"
    )
    py_sets_entity_subscription = GenericMetricsSetsSubscription
    assert issubclass(config_sets_entity_subscription, ComplexEntitySubscription)
    assert config_sets_entity_subscription.name == "generic_metrics_sets_subscription"

    config_sets = config_sets_entity_subscription(data_dict=data)
    py_sets = py_sets_entity_subscription(data_dict=data)

    assert config_sets.name == "generic_metrics_sets_subscription"

    assert config_sets.MAX_ALLOWED_AGGREGATIONS == py_sets.MAX_ALLOWED_AGGREGATIONS
    assert config_sets.disallowed_aggregations == py_sets.disallowed_aggregations
    assert (
        config_sets.get_entity_subscription_conditions_for_snql()
        == py_sets.get_entity_subscription_conditions_for_snql()
    )


def test_build_simple_entity_subscription_from_config() -> None:
    config_sets_entity_subscription: Type[
        Union[PluggableEntitySubscriptionComplex, PluggableEntitySubscriptionSimple]
    ] = build_entity_subscription_from_config(
        "snuba/datasets/configuration/transactions/entity_subscriptions/transactions.yaml"
    )
    py_transactions_entity_subscription = TransactionsSubscription
    assert issubclass(config_sets_entity_subscription, SimpleEntitySubscription)
    assert config_sets_entity_subscription.name == "transactions_subscription"

    config_sets = config_sets_entity_subscription(data_dict=data)
    py_sets = py_transactions_entity_subscription(data_dict=data)

    assert config_sets.name == "transactions_subscription"

    assert config_sets.MAX_ALLOWED_AGGREGATIONS == py_sets.MAX_ALLOWED_AGGREGATIONS
    assert config_sets.disallowed_aggregations == py_sets.disallowed_aggregations
    assert (
        config_sets.get_entity_subscription_conditions_for_snql()
        == py_sets.get_entity_subscription_conditions_for_snql()
    )


def test_bad_configuration_broken_attribute() -> None:
    with pytest.raises((ValidationError, TypeError)):
        build_entity_subscription_from_config(
            "tests/subscriptions/entity_subscriptions/configuration/broken_entity_subscription_bad_attribute.yaml"
        )
