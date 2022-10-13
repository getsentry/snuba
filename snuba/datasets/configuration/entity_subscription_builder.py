import logging
from typing import Type

from snuba.datasets.configuration.json_schema import V1_ENTITY_SUBSCIPTION_SCHEMA
from snuba.datasets.configuration.loader import load_configuration_data
from snuba.datasets.entity_subscriptions.pluggable_entity_subscription import (
    PluggableEntitySubscription,
)

logger = logging.getLogger("snuba.entity_subscriptions_builder")


def build_entity_subscription_from_config(
    file_path: str,
) -> Type[PluggableEntitySubscription]:
    config_data = load_configuration_data(
        file_path, {"entity_subscription": V1_ENTITY_SUBSCIPTION_SCHEMA}
    )
    PluggableEntitySubscription.name = config_data["name"]
    PluggableEntitySubscription.MAX_ALLOWED_AGGREGATIONS = config_data[
        "max_allowed_aggregations"
    ]
    PluggableEntitySubscription.disallowed_aggregations = config_data[
        "disallowed_aggregations"
    ]
    return PluggableEntitySubscription
