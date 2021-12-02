from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import Generic, Iterator, Tuple, TypeVar

from snuba.subscriptions.utils import Tick
from snuba.utils.types import Interval

TTask = TypeVar("TTask")


@dataclass(frozen=True)
class ScheduledTask(Generic[TTask]):
    """
    A scheduled task represents a unit of work (a task) that is intended to
    be executed at (or around) a specific time.
    """

    # The time that this task was scheduled to execute.
    timestamp: datetime

    # The task that should be executed.
    task: TTask


class Scheduler(ABC, Generic[TTask]):
    """
    The scheduler maintains the scheduling state for various tasks and
    provides the ability to query the schedule to find tasks that were
    scheduled between a time interval.
    """

    @abstractmethod
    def find(self, interval: Interval[datetime]) -> Iterator[ScheduledTask[TTask]]:
        """
        Find all of the tasks that were scheduled to be executed between the
        lower bound (exclusive) and upper bound (inclusive) of the provided
        interval. The tasks returned should be ordered by timestamp in
        ascending order.

        Will be deprecated once moving to the new subscriptions pipeline.
        """
        raise NotImplementedError

    @abstractmethod
    def find_with_tick(self, tick: Tick) -> Iterator[ScheduledTask[Tuple[TTask, Tick]]]:
        """
        Like `find()` but includes the tick. Required for the
        new subscriptions pipeline.
        """
        raise NotImplementedError
