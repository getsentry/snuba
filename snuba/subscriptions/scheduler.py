import math
from abc import ABC
from datetime import datetime, timedelta
from typing import Iterator, List, Optional

from snuba.subscriptions.data import PartitionId, Subscription, SubscriptionIdentifier
from snuba.subscriptions.store import SubscriptionDataStore
from snuba.utils.metrics import MetricsBackend
from snuba.utils.scheduler import ScheduledTask, Scheduler
from snuba.utils.types import Interval


class SubscriptionFilter(ABC):
    """
    Decides when to run a subscription or when to defer it.
    """

    def filter(self, subscription: Subscription, timestamp: int) -> bool:
        raise NotImplementedError


class ImmediateSubscriptionFilter(SubscriptionFilter):
    """
    Schedules a subscription as soon as possible
    """

    def filter(self, subscription: Subscription, timestamp: int) -> bool:
        resolution = int(subscription.data.resolution.total_seconds())
        return timestamp % resolution == 0


class SubscriptionScheduler(Scheduler[Subscription]):
    def __init__(
        self,
        store: SubscriptionDataStore,
        partition_id: PartitionId,
        cache_ttl: timedelta,
        metrics: MetricsBackend,
    ) -> None:
        self.__store = store
        self.__cache_ttl = cache_ttl
        self.__partition_id = partition_id
        self.__metrics = metrics

        self.__subscriptions: List[Subscription] = []
        self.__last_refresh: Optional[datetime] = None
        self.__filter = ImmediateSubscriptionFilter()

    def __get_subscriptions(self) -> List[Subscription]:
        current_time = datetime.now()

        if (
            self.__last_refresh is None
            or (current_time - self.__last_refresh) > self.__cache_ttl
        ):
            self.__subscriptions = [
                Subscription(SubscriptionIdentifier(self.__partition_id, uuid), data)
                for uuid, data in self.__store.all()
            ]
            self.__last_refresh = current_time
            self.__metrics.gauge(
                "schedule.size",
                len(self.__subscriptions),
                tags={"partition": str(self.__partition_id)},
            )

        self.__metrics.timing(
            "schedule.staleness",
            (current_time - self.__last_refresh).total_seconds() * 1000.0,
            tags={"partition": str(self.__partition_id)},
        )

        return self.__subscriptions

    def find(
        self, interval: Interval[datetime]
    ) -> Iterator[ScheduledTask[Subscription]]:
        subscriptions = self.__get_subscriptions()

        count = 0
        for timestamp in range(
            math.ceil(interval.lower.timestamp()),
            math.ceil(interval.upper.timestamp()),
        ):
            for subscription in subscriptions:
                if self.__filter.filter(subscription, timestamp):
                    count += 1
                    yield ScheduledTask(datetime.fromtimestamp(timestamp), subscription)

        if count:
            self.__metrics.increment("metrics_scheduled", count)
