from typing import Sequence

from snuba.datasets.entity_subscriptions.entity_subscription import (
    ComplexEntitySubscription,
    SimpleEntitySubscription,
)


class PluggableEntitySubscriptionComplex(ComplexEntitySubscription):
    """
    PluggableEntitySubscriptionComplex is a version of EntitySubscription that is design to be populated by
    static YAML-based configuration files.

    TODO: This temporary and always extends ComplexEntitySubscription for now.
    """

    name: str
    MAX_ALLOWED_AGGREGATIONS: int
    disallowed_aggregations: Sequence[str]


class PluggableEntitySubscriptionSimple(SimpleEntitySubscription):
    """
    PluggableEntitySubscriptionSimple is a version of EntitySubscription that is design to be populated by
    static YAML-based configuration files.

    TODO: This temporary and always extends ComplexEntitySubscription for now.
    """

    name: str
    MAX_ALLOWED_AGGREGATIONS: int
    disallowed_aggregations: Sequence[str]
