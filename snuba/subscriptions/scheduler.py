import math
from datetime import datetime, timedelta
from typing import Iterator, List, Optional

from snuba.subscriptions.data import PartitionId, Subscription, SubscriptionIdentifier
from snuba.subscriptions.store import SubscriptionDataStore
from snuba.utils.metrics.backends.abstract import MetricsBackend
from snuba.utils.scheduler import Scheduler, ScheduledTask
from snuba.utils.types import Interval


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

        for timestamp in range(
            math.ceil(interval.lower.timestamp()),
            math.ceil(interval.upper.timestamp()),
        ):
            for subscription in subscriptions:
                resolution = int(subscription.data.resolution.total_seconds())
                if timestamp % resolution == 0:
                    yield ScheduledTask(datetime.fromtimestamp(timestamp), subscription)
