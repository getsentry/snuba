from __future__ import annotations

import logging
from enum import Enum
from typing import NamedTuple

from snuba.subscriptions.types import Interval

logger = logging.getLogger(__name__)


class SchedulingWatermarkMode(Enum):
    PARTITION = "partition"
    GLOBAL = "global"


class Tick(NamedTuple):
    partition: int
    offsets: Interval[int]
    timestamps: Interval[float]

    def time_shift(self, delta: float) -> Tick:
        """
        Returns a new ``Tick`` instance that has had the bounds of its time
        interval shifted by the provided delta.
        """
        return Tick(
            self.partition,
            self.offsets,
            Interval(self.timestamps.lower + delta, self.timestamps.upper + delta),
        )
