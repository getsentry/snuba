import math
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from typing import Generic, Iterator, List, Mapping, Optional, TypeVar

from snuba import settings
from snuba.subscriptions.data import PartitionId, Subscription, SubscriptionIdentifier
from snuba.subscriptions.store import SubscriptionDataStore
from snuba.utils.metrics import MetricsBackend
from snuba.utils.scheduler import ScheduledTask, Scheduler
from snuba.utils.types import Interval

TSubscription = TypeVar("TSubscription")


class TaskBuilder(ABC, Generic[TSubscription]):
    """
    Takes a Subscription and a timestamp, decides whether we should
    schedule that task at the current timestamp and provides the
    task instance.
    """

    @abstractmethod
    def get_task(
        self, subscription: Subscription, timestamp: int
    ) -> Optional[ScheduledTask[TSubscription]]:
        raise NotImplementedError

    @abstractmethod
    def reset_metrics(self) -> Mapping[str, int]:
        raise NotImplementedError


class ImmediateTaskBuilder(TaskBuilder[Subscription]):
    """
    Schedules a subscription as soon as possible
    """

    def __init__(self) -> None:
        self.__count = 0

    def get_task(
        self, subscription: Subscription, timestamp: int
    ) -> Optional[ScheduledTask[Subscription]]:
        resolution = int(subscription.data.resolution.total_seconds())
        if timestamp % resolution == 0:
            return ScheduledTask(datetime.fromtimestamp(timestamp), subscription)
        else:
            return None

    def reset_metrics(self) -> Mapping[str, int]:
        metrics = {"tasks.built": self.__count}
        self.__count = 0
        return metrics


class JitteredTaskBuilder(TaskBuilder[Subscription]):
    """
    Schedules subscriptions applying a jitter to distribute subscriptions
    evenly in the resolution period.

    Each subscription is assigned a stable jitter which correspond to a
    timestamp in the resolution interval.
    For example if a subscription has a 60 seconds resolution, the jitter
    is calculated from the subscription id (to stay constant) and it is
    a number between 0 and 59.

    That subscription is then scheduled when the timestamp % 60 is equal
    to the jitter instead of being equal to 0.
    This would spread the subscription evenly.

    There is a setting to define the maximum resolution the jitter applies.

    The time range of the query does not include the jitter. So if the
    jitter for a 60 seconds resolution query is 4 seconds. The query is
    scheduled 4 seconds after the beginning of the minute, but the time
    range of the query is still aligned with the minute.
    """

    def __init__(self) -> None:
        self.__count = 0

    def get_task(
        self, subscription: Subscription, timestamp: int
    ) -> Optional[ScheduledTask[Subscription]]:
        max_resolution = settings.MAX_RESOLUTION_FOR_JITTER
        resolution = int(subscription.data.resolution.total_seconds())

        if resolution > max_resolution:
            if timestamp % resolution == 0:
                return ScheduledTask(datetime.fromtimestamp(timestamp), subscription)
            else:
                return None

        jitter = subscription.identifier.uuid.int % resolution
        if timestamp % resolution == jitter:
            return ScheduledTask(
                datetime.fromtimestamp(timestamp - jitter), subscription
            )
        else:
            return None

    def reset_metrics(self) -> Mapping[str, int]:
        metrics = {"tasks.built": self.__count}
        self.__count = 0
        return metrics


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
        self.__builder = ImmediateTaskBuilder()

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
                task = self.__builder.get_task(subscription, timestamp)
                if task is not None:
                    yield task

        metrics = self.__builder.reset_metrics()
        if any(c for m, c in metrics.items() if c > 0):
            for metric, count in metrics.items():
                self.__metrics.increment("metrics.scheduled", count)
