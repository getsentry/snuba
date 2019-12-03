from abc import ABC, abstractmethod
from typing import Generic, Iterator, Tuple, TypeVar

from snuba.utils.types import Interval
from snuba.subscriptions.types import TTimestamp


TTask = TypeVar("TTask")


class Scheduler(ABC, Generic[TTimestamp, TTask]):
    @abstractmethod
    def find(
        self, interval: Interval[TTimestamp]
    ) -> Iterator[Tuple[TTimestamp, TTask]]:
        """
        Return all of the tasks that were scheduled to be executed between
        the lower bound (exclusive) and upper bound (inclusive) of the
        provided interval.
        """
        raise NotImplementedError
