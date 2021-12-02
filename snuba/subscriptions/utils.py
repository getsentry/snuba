from __future__ import annotations

from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Mapping, NamedTuple

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

    def to_dict(self) -> Mapping[str, Any]:
        return {
            "partition": self.partition,
            "offsets": [self.offsets.lower, self.offsets.upper],
            "timestamps": [
                self.timestamps.lower.isoformat(),
                self.timestamps.upper.isoformat(),
            ],
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> Tick:
        return cls(
            data["partition"],
            Interval(data["offsets"][0], data["offsets"][1]),
            Interval(
                datetime.fromisoformat(data["timestamps"][0]),
                datetime.fromisoformat(data["timestamps"][1]),
            ),
        )
