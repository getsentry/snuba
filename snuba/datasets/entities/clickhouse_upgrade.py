from enum import Enum
from random import random
from typing import NamedTuple, Optional, cast

from snuba.state import get_config


class Option(Enum):
    ERRORS = 1
    ERRORS_V2 = 2


class Choice(NamedTuple):
    primary: Option
    secondary: Optional[Option]


class RolloutSelector:
    """
    Takes the rollout decisions during the upgrade to Clickhouse 21.8.
    This class assumes that there are two storages to choose from before
    running a query.
    The main pipeline builder needs a decision on which queries to run
    (both storages or only one) and which result to trust.

    This class can return one storage (which is the only one the query
    will run onto) or two storages, in that case the first will be the
    trusted one.

    There are multiple way to configure this rollout:
    First choice: which query should be trusted:
    - check runtime config if the default secondary should be trusted
    - check global rollout if the default secondary should be trusted
    Now we know which query will be trusted. hould we run the second ?
    Check the configs and settings in the same order.
    """

    def __init__(
        self, default_primary: Option, default_secondary: Option, config_prefix: str
    ) -> None:
        self.__default_primary = default_primary
        self.__default_secondary = default_secondary
        self.__config_prefix = config_prefix

    def __is_query_rolled_out(
        self, referrer: str, config_referrer_prefix: str, general_rollout_config: str,
    ) -> bool:
        rollout_percentage = get_config(
            f"rollout_upgraded_{self.__config_prefix}_{config_referrer_prefix}_{referrer}",
            None,
        )
        if rollout_percentage is None:
            rollout_percentage = get_config(general_rollout_config, 0.0)

        return random() <= cast(float, rollout_percentage)

    def choose(self, referrer: str) -> Choice:
        trust_secondary = self.__is_query_rolled_out(
            referrer, "trust", f"rollout_upgraded_{self.__config_prefix}_trust",
        )

        execute_both = self.__is_query_rolled_out(
            referrer, "execute", f"rollout_upgraded_{self.__config_prefix}_execute",
        )

        primary = (
            self.__default_secondary if trust_secondary else self.__default_primary
        )
        if not execute_both:
            return Choice(primary, None)
        else:
            return Choice(
                primary,
                self.__default_secondary
                if not trust_secondary
                else self.__default_primary,
            )
