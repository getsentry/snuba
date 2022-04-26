from __future__ import annotations

import logging
from datetime import datetime, timedelta
from enum import Enum
from typing import Literal, NamedTuple, Optional, Set, Union

from snuba import state
from snuba.utils.types import Interval

logger = logging.getLogger(__name__)


class SchedulingWatermarkMode(Enum):
    PARTITION = "partition"
    GLOBAL = "global"


class Tick(NamedTuple):
    # TODO: Partition is only optional in the old subscription worker, once
    # that is deprecated, partition should no longer be optional
    partition: Optional[int]
    offsets: Interval[int]
    timestamps: Interval[datetime]

    def time_shift(self, delta: timedelta) -> Tick:
        """
        Returns a new ``Tick`` instance that has had the bounds of its time
        interval shifted by the provided delta.
        """
        return Tick(
            self.partition,
            self.offsets,
            Interval(self.timestamps.lower + delta, self.timestamps.upper + delta),
        )


class SubscriptionMode(Enum):
    LEGACY = "legacy"
    NEW = "new"
    BOTH = "both"
    TRANSITION_LEGACY = "transition_legacy"
    TRANSITION_NEW = "transition_new"


def run_new_pipeline(entity_name: str, scheduled_ts: datetime) -> bool:
    return SubscriptionMode.NEW in _select_pipelines(entity_name, scheduled_ts)


def run_legacy_pipeline(entity_name: str, scheduled_ts: datetime) -> bool:
    return SubscriptionMode.LEGACY in _select_pipelines(entity_name, scheduled_ts)


def _select_pipelines(
    entity_name: str, scheduled_ts: datetime
) -> Set[Union[Literal[SubscriptionMode.LEGACY], Literal[SubscriptionMode.NEW]]]:
    """
    Use to transition between the old and new subscription pipelines without
    missing any (or duplicating for that matter) query execution and subscription
    results.

    The are five modes which can be set:
    - legacy (the default)
    - new
    - both
    - transition_legacy
    - transition_new

    If transition_legacy or transition_new are being used a transition timestamp
    (Unix time) must be provided. Any subscriptions scheduled up to the transition
    time will use the prior pipeline and any subscriptions scheduled after the
    transition time will use the pipeline being transitioned to.
    """
    try:
        mode = SubscriptionMode(
            state.get_config(f"subscription_mode_{entity_name}", "legacy")
        )

        transition_time: Optional[int] = state.get_config(
            f"subscription_transition_time_{entity_name}"
        )

        if mode == SubscriptionMode.BOTH:
            return {SubscriptionMode.NEW, SubscriptionMode.LEGACY}

        if mode == SubscriptionMode.NEW:
            return {SubscriptionMode.NEW}

        if mode == SubscriptionMode.LEGACY:
            return {SubscriptionMode.LEGACY}

        if mode == SubscriptionMode.TRANSITION_NEW:
            assert transition_time is not None
            run_new = datetime.timestamp(scheduled_ts) < transition_time

        if mode == SubscriptionMode.TRANSITION_LEGACY:
            assert transition_time is not None
            run_new = datetime.timestamp(scheduled_ts) >= transition_time

        if run_new:
            return {SubscriptionMode.NEW}
        else:
            return {SubscriptionMode.LEGACY}

    except Exception as e:
        # If things failed for any reason (e.g. invalid config) fall back to legacy
        logger.warning("Failed to evaluate subscription mode", exc_info=e)
        return {SubscriptionMode.LEGACY}
    raise
