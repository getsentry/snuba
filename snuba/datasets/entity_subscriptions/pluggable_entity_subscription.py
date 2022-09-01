from typing import Sequence

from snuba.datasets.entity_subscriptions.entity_subscription import SessionsSubscription


class PluggableEntitySubscription(
    SessionsSubscription
):  # always extends SessionsSubscription for now
    """
    PluggableEntitySubscription is a version of EntitySubscription that is design to be populated by
    static YAML-based configuration files.
    """

    name: str
    MAX_ALLOWED_AGGREGATIONS: int
    disallowed_aggregations: Sequence[str]
