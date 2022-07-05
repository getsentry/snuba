from __future__ import annotations

import logging
from datetime import datetime, timedelta
from enum import Enum
from typing import NamedTuple, Optional

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
