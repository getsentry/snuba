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
    - if a dataset is rolled out then use ast
    - if the referrer is rolled out for a dataset then use ast
    - if none of the above decide according to the master
        rollout percentage
    """

    if get_config(KILLSWITCH_CONFIG, 0):
        return False

    current_percentage = random.random() * 100
    dataset_rollout_percentage = settings.AST_DATASET_ROLLOUT.get(dataset_name, None)
    if dataset_rollout_percentage is not None:
        if current_percentage < dataset_rollout_percentage:
            return True

    referrer_rollout_config = settings.AST_REFERRER_ROLLOUT.get(dataset_name, None)
    if referrer_rollout_config:
        if referrer is None:
            # Treat no referrer as empty string. So we can configure
            # a percentage for queries without referrer.
            referrer = ""
        referrer_percentage = referrer_rollout_config.get(referrer, None)
        if referrer_percentage is not None and current_percentage < referrer_percentage:
            return True

    rollout_rate = get_config(ROLLOUT_RATE_CONFIG, 0)
    if rollout_rate is None:
        # This is for mypy since it does not believe
        # (rightfully) that rollout_rate is an int.
        return False
    return current_percentage < int(rollout_rate)
