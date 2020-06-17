from typing import Optional
import random

from snuba import settings
from snuba.state import get_config

ROLLOUT_RATE_CONFIG = "ast_rollout_rate"
KILLSWITCH_CONFIG = "ast_shutdown"


def is_ast_rolled_out(dataset_name: str, referrer: Optional[str]) -> bool:
    """
    Applies some rules to decide whether we should run a query based
    on the AST or on the legacy representation.

    - if the emergency killswitch is on, then no rollout
    - if the referrer percentage is configured for the requested dataset
      honor that percentage
    - if a dataset is configured, then honor that percentage
    - if none of the above decide according to the master
        rollout percentage
    """

    if get_config(KILLSWITCH_CONFIG, 0):
        return False

    current_percentage = random.random() * 100
    referrer_rollout_config = settings.AST_REFERRER_ROLLOUT.get(dataset_name, None)
    if referrer_rollout_config:
        referrer_percentage = referrer_rollout_config.get(referrer, None)
        if referrer_percentage is not None:
            return current_percentage < referrer_percentage

    dataset_rollout_percentage = settings.AST_DATASET_ROLLOUT.get(dataset_name, None)
    if dataset_rollout_percentage is not None:
        return current_percentage < dataset_rollout_percentage

    rollout_rate = get_config(ROLLOUT_RATE_CONFIG, 0)
    if rollout_rate is None:
        # This is for mypy since it does not believe
        # (rightfully) that rollout_rate is an int.
        return False
    return current_percentage < int(rollout_rate)
