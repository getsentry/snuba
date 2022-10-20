import logging
from typing import Type, Union

import sentry_sdk

from snuba.datasets.configuration.json_schema import V1_ENTITY_SUBSCIPTION_SCHEMA
from snuba.datasets.configuration.loader import load_configuration_data
from snuba.datasets.entity_subscriptions.pluggable_entity_subscription import (
    PluggableEntitySubscriptionComplex,
    PluggableEntitySubscriptionSimple,
)

logger = logging.getLogger("snuba.entity_subscriptions_builder")


def build_entity_subscription_from_config(
    file_path: str,
) -> Type[Union[PluggableEntitySubscriptionComplex, PluggableEntitySubscriptionSimple]]:
    config = load_configuration_data(
        file_path, {"entity_subscription": V1_ENTITY_SUBSCIPTION_SCHEMA}
    )

    with sentry_sdk.start_span(
        op="build", description=f"Entity Subscription: {config['name']}"
    ):
        # Default to simple entity subscription
        PluggableEntitySubscription: Type[
            Union[PluggableEntitySubscriptionComplex, PluggableEntitySubscriptionSimple]
        ] = PluggableEntitySubscriptionSimple

        if "type" in config and config["type"] == "complex":
            PluggableEntitySubscription = PluggableEntitySubscriptionComplex

        PluggableEntitySubscription.name = config["name"]
        if "max_allowed_aggregations" in config:
            PluggableEntitySubscription.MAX_ALLOWED_AGGREGATIONS = config[
                "max_allowed_aggregations"
            ]
        if "disallowed_aggregations" in config:
            PluggableEntitySubscription.disallowed_aggregations = config[
                "disallowed_aggregations"
            ]
    return PluggableEntitySubscription
