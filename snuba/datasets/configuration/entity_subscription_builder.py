import logging
from typing import Type

from snuba.datasets.configuration.json_schema import ENIITY_SUBSCRIPTION_VALIDATORS
from snuba.datasets.configuration.loader import load_configuration_data
from snuba.datasets.entity_subscriptions.pluggable_entity_subscription import (
    PluggableEntitySubscription,
)

logger = logging.getLogger("snuba.entity_subscriptions_builder")


def build_entity_subscription_from_config(
    file_path: str,
) -> Type[PluggableEntitySubscription]:
    config = load_configuration_data(file_path, ENIITY_SUBSCRIPTION_VALIDATORS)
    PluggableEntitySubscription.name = config["name"]
    PluggableEntitySubscription.MAX_ALLOWED_AGGREGATIONS = config[
        "max_allowed_aggregations"
    ]
    PluggableEntitySubscription.disallowed_aggregations = config[
        "disallowed_aggregations"
    ]
    return PluggableEntitySubscription
