import logging
from typing import Mapping, Type

import sentry_sdk

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
    config = load_configuration_data(
        file_path, {"entity_subscription": V1_ENTITY_SUBSCIPTION_SCHEMA}
    )
    with sentry_sdk.start_span(
        op="function", description="Build Entity Subscription"
    ) as span:
        span.set_tag("entity_subscription", config["name"])

        PluggableEntitySubscription.name = config["name"]
        PluggableEntitySubscription.MAX_ALLOWED_AGGREGATIONS = config[
            "max_allowed_aggregations"
        ]
        PluggableEntitySubscription.disallowed_aggregations = config[
            "disallowed_aggregations"
        ]
        return PluggableEntitySubscription
