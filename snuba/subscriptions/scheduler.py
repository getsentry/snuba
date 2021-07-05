import math
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from typing import Generic, Iterator, List, Mapping, Optional, Sequence, Tuple, TypeVar

from snuba import settings, state
from snuba.subscriptions.data import PartitionId, Subscription, SubscriptionIdentifier
from snuba.subscriptions.store import SubscriptionDataStore
from snuba.utils.metrics import MetricsBackend
from snuba.utils.scheduler import ScheduledTask, Scheduler
from snuba.utils.types import Interval

TSubscription = TypeVar("TSubscription")

Tags = Mapping[str, str]


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
    def reset_metrics(self) -> Sequence[Tuple[str, int, Tags]]:
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
            self.__count += 1
            return ScheduledTask(datetime.fromtimestamp(timestamp), subscription)
        else:
            return None

    def reset_metrics(self) -> Sequence[Tuple[str, int, Tags]]:
        metrics: Sequence[Tuple[str, int, Tags]] = [("tasks.built", self.__count, {})]
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
        self.__count_max_resolution = 0

    def get_task(
        self, subscription: Subscription, timestamp: int
    ) -> Optional[ScheduledTask[Subscription]]:
        max_resolution = settings.MAX_RESOLUTION_FOR_JITTER
        resolution = int(subscription.data.resolution.total_seconds())

        if resolution > max_resolution:
            if timestamp % resolution == 0:
                self.__count += 1
                self.__count_max_resolution += 1
                return ScheduledTask(datetime.fromtimestamp(timestamp), subscription)
            else:
                return None

        jitter = subscription.identifier.uuid.int % resolution
        if timestamp % resolution == jitter:
            self.__count += 1
            return ScheduledTask(
                datetime.fromtimestamp(timestamp - jitter), subscription
            )
        else:
            return None

    def reset_metrics(self) -> Sequence[Tuple[str, int, Tags]]:
        metrics: Sequence[Tuple[str, int, Tags]] = [
            ("tasks.built", self.__count, {}),
            ("tasks.above.resolution", self.__count_max_resolution, {}),
        ]
        self.__count = 0
        self.__count_max_resolution = 0
        return metrics


class DelegateTaskBuilder(TaskBuilder[Subscription]):
    """
    A delegate capable of switching back and forth between the
    immediate and jittered task builders according to runtime
    settings.
    """

    def __init__(self) -> None:
        self.__immediate_builder = ImmediateTaskBuilder()
        self.__jittered_builder = JitteredTaskBuilder()

    def get_task(
        self, subscription: Subscription, timestamp: int
    ) -> Optional[ScheduledTask[Subscription]]:
        immediate_task = self.__immediate_builder.get_task(subscription, timestamp)
        jittered_task = self.__jittered_builder.get_task(subscription, timestamp)
        primary_builder = state.get_config(
            "subscription_primary_task_builder", "immediate"
        )
        if primary_builder == "jittered":
            return jittered_task
        else:
            return immediate_task

    def reset_metrics(self) -> Sequence[Tuple[str, int, Tags]]:
        def add_tag(tags: Tags, builder_type: str) -> Tags:
            return {
                **tags,
                "type": builder_type,
            }

        immediate_metrics = self.__immediate_builder.reset_metrics()
        immediate_tagged = [
            (metric[0], metric[1], add_tag(metric[2], "immediate"))
            for metric in immediate_metrics
        ]
        jittered_metrics = self.__jittered_builder.reset_metrics()
        jittered_tagged = [
            (metric[0], metric[1], add_tag(metric[2], "jittered"))
            for metric in jittered_metrics
        ]

        return [
            *immediate_tagged,
            *jittered_tagged,
        ]


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
        self.__builder = DelegateTaskBuilder()

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
        if any(metric for metric in metrics if metric[1] > 0):
            for metric in metrics:
                self.__metrics.increment(metric[0], metric[1], tags=metric[2])
