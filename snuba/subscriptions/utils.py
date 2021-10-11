from __future__ import annotations

from datetime import datetime, timedelta
from enum import Enum
from typing import NamedTuple

from snuba.utils.types import Interval


class SchedulingWatermarkMode(Enum):
    PARTITION = "partition"
    GLOBAL = "global"


class Tick(NamedTuple):
    partition: int
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


from enum import Enum
