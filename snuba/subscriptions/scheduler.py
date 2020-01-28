import math
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Generic, Iterator, List, Optional, TypeVar

from snuba.subscriptions.data import PartitionId, Subscription, SubscriptionIdentifier
from snuba.subscriptions.store import SubscriptionDataStore
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


class SubscriptionScheduler(Scheduler[Subscription]):
    def __init__(
        self,
        store: SubscriptionDataStore,
        partition_id: PartitionId,
        cache_ttl: timedelta,
    ) -> None:
        self.__store = store
        self.__cache_ttl = cache_ttl
        self.__partition_id = partition_id
        self.__subscriptions: List[Subscription] = []
        self.__last_refresh: Optional[datetime] = None

    def __get_subscriptions(self, current_time: datetime) -> List[Subscription]:
        if (
            self.__last_refresh is None
            or (current_time - self.__last_refresh) > self.__cache_ttl
        ):
            self.__subscriptions = [
                Subscription(SubscriptionIdentifier(self.__partition_id, uuid), data)
                for uuid, data in self.__store.all()
            ]
            self.__last_refresh = current_time
        return self.__subscriptions

    def find(
        self, interval: Interval[datetime]
    ) -> Iterator[ScheduledTask[Subscription]]:
        subscriptions = self.__get_subscriptions(interval.lower)
        for i in range(
            math.ceil(interval.lower.timestamp()),
            math.ceil(interval.upper.timestamp()),
        ):
            for subscription in subscriptions:
                resolution = int(subscription.data.resolution.total_seconds())
                if i % resolution == 0:
                    yield ScheduledTask(datetime.fromtimestamp(i), subscription)
