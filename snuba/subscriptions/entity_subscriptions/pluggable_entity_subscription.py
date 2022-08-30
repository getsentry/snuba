from dataclasses import dataclass
from typing import Sequence


@dataclass
class PluggableEntitySubscription:
    """
    PluggableEntitySubscription is a version of EntitySubscription that is design to be populated by
    static YAML-based configuration files.
    """

    MAX_ALLOWED_AGGREGATIONS: int
    disallowed_aggregations: Sequence[str]
