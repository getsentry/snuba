from dataclasses import dataclass
from datetime import datetime
from typing import TypeVar, Generic

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
