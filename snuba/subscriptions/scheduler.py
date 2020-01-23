import math
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Generic, Iterator, NamedTuple, NewType, TypeVar
from uuid import UUID

from snuba.subscriptions.data import Subscription as SubscriptionData
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
        """
        raise NotImplementedError


# XXX: This is all temporary until we get these types properly introduced
PartitionId = NewType("PartitionId", int)


@dataclass(frozen=True)
class SubscriptionIdentifier:
    partition: PartitionId
    uuid: UUID


class Subscription(NamedTuple):
    identifier: SubscriptionIdentifier
    data: SubscriptionData


class SubscriptionScheduler(Scheduler[Subscription]):
    def __init__(self) -> None:
        self.__subscriptions: Dict[UUID, Subscription] = {}

    def set(self, value: Subscription) -> None:
        self.__subscriptions[value.identifier.uuid] = value

    def delete(self, key: UUID) -> None:
        self.__subscriptions.pop(key, None)

    def clear(self) -> None:
        # XXX: Not part of the standard interface. Since we want to periodically clear
        # the cache, whatever is managing this should probably clear and refetch
        self.__subscriptions = {}

    def find(
        self, interval: Interval[datetime]
    ) -> Iterator[ScheduledTask[Subscription]]:
        for uuid, subscription in self.__subscriptions.items():
            resolution = subscription.data.resolution.total_seconds()
            for i in range(
                math.ceil(interval.lower.timestamp() / resolution),
                math.ceil(interval.upper.timestamp() / resolution),
            ):
                yield ScheduledTask(
                    datetime.fromtimestamp(i * resolution), subscription,
                )
