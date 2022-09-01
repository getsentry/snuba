from typing import Sequence


class PluggableEntitySubscription:
    """
    PluggableEntitySubscription is a version of EntitySubscription that is design to be populated by
    static YAML-based configuration files.
    """

    name: str
    MAX_ALLOWED_AGGREGATIONS: int
    disallowed_aggregations: Sequence[str]
