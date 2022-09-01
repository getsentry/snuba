import logging
from typing import Mapping, Type

from snuba.datasets.configuration.json_schema import V1_ENTITY_SUBSCIPTION_SCHEMA
from snuba.datasets.configuration.loader import load_configuration_data
from snuba.datasets.entity_subscriptions.entity_subscription import (
    BaseEventsSubscription,
    EntitySubscription,
    SessionsSubscription,
)
from snuba.datasets.entity_subscriptions.pluggable_entity_subscription import (
    PluggableEntitySubscription,
)

logger = logging.getLogger("snuba.entity_subscriptions_builder")


_PARENT_SUBSCRIPTION_CLASS_MAPPING: Mapping[str, Type[EntitySubscription]] = {
    "base_events_subscription": BaseEventsSubscription,
    "sessions_subscription": SessionsSubscription,
}


def build_entity_subscription_from_config(
    file_path: str,
) -> Type[PluggableEntitySubscription]:
    logger.info(f"building entity from {file_path}")
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
