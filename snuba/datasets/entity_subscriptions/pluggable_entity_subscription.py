from typing import Sequence

from snuba.datasets.entity_subscriptions.entity_subscription import SessionsSubscription


class PluggableEntitySubscription(SessionsSubscription):
    """
    PluggableEntitySubscription is a version of EntitySubscription that is design to be populated by
    static YAML-based configuration files.

    TODO: This always extends SessionsSubscription for now.
    We will need to add functionality to extend other base classes in the future
    """

    name: str
    MAX_ALLOWED_AGGREGATIONS: int
    disallowed_aggregations: Sequence[str]
