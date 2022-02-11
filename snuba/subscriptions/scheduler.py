import math
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from enum import Enum
from typing import (
    Iterator,
    List,
    Mapping,
    MutableMapping,
    Optional,
    Sequence,
    Tuple,
    TypeVar,
)

from snuba import settings, state
from snuba.datasets.entities import EntityKey
from snuba.subscriptions.data import (
    PartitionId,
    ScheduledSubscriptionTask,
    Subscription,
    SubscriptionIdentifier,
)
from snuba.subscriptions.data import SubscriptionScheduler as SubscriptionSchedulerBase
from snuba.subscriptions.data import SubscriptionWithMetadata
from snuba.subscriptions.store import SubscriptionDataStore
from snuba.subscriptions.utils import Tick
from snuba.utils.metrics import MetricsBackend

TSubscription = TypeVar("TSubscription")

Tags = Mapping[str, str]


class TaskBuilder(ABC):
    """
    Takes a Subscription and a timestamp, decides whether we should
    schedule that task at the current timestamp and provides the
    task instance.
    """

    @abstractmethod
    def get_task(
        self, subscription_with_metadata: SubscriptionWithMetadata, timestamp: int
    ) -> Optional[ScheduledSubscriptionTask]:
        raise NotImplementedError

    @abstractmethod
    def reset_metrics(self) -> Sequence[Tuple[str, int, Tags]]:
        raise NotImplementedError


class ImmediateTaskBuilder(TaskBuilder):
    """
    Schedules a subscription as soon as possible
    """

    def __init__(self) -> None:
        self.__count = 0

    def get_task(
        self, subscription_with_metadata: SubscriptionWithMetadata, timestamp: int
    ) -> Optional[ScheduledSubscriptionTask]:
        subscription = subscription_with_metadata.subscription

        resolution = int(subscription.data.resolution.total_seconds())
        if timestamp % resolution == 0:
            self.__count += 1
            return ScheduledSubscriptionTask(
                datetime.fromtimestamp(timestamp), subscription_with_metadata
            )
        else:
            return None

    def reset_metrics(self) -> Sequence[Tuple[str, int, Tags]]:
        metrics: Sequence[Tuple[str, int, Tags]] = [("tasks.built", self.__count, {})]
        self.__count = 0
        return metrics


class JitteredTaskBuilder(TaskBuilder):
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
        self, subscription_with_metadata: SubscriptionWithMetadata, timestamp: int
    ) -> Optional[ScheduledSubscriptionTask]:
        subscription = subscription_with_metadata.subscription

        resolution = int(subscription.data.resolution.total_seconds())

        if resolution > settings.MAX_RESOLUTION_FOR_JITTER:
            if timestamp % resolution == 0:
                self.__count += 1
                self.__count_max_resolution += 1
                return ScheduledSubscriptionTask(
                    datetime.fromtimestamp(timestamp), subscription_with_metadata
                )
            else:
                return None

        jitter = subscription.identifier.uuid.int % resolution
        if timestamp % resolution == jitter:
            self.__count += 1
            return ScheduledSubscriptionTask(
                datetime.fromtimestamp(timestamp - jitter), subscription_with_metadata
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


class TaskBuilderMode(Enum):
    IMMEDIATE = "immediate"
    JITTERED = "jittered"
    TRANSITION_JITTER = "transition_jitter"
    TRANSITION_IMMEDIATE = "transition_immediate"


class TaskBuilderModeState:
    """
    Manages task building mode transitions.

    There are two final mode: immediate and jittered that correspond to
    two different implementations of the TaskBuilder.

    The current mode is defined as runtime configuration, though we do
    not want to transition from one mode to the other at any timestamp.
    We want for transitions to happen only at timestamps that are
    multiple of the subscription resolution.

    This class manages those transitions by adding two modes:
    TRANSITION_JITTERED and TRANSITION_IMMEDIATE. When the system is in
    a transition mode, this class will return the previous state
    up until the timestamp reaches the end of the resolution time
    and then it will start returning the new mode.
    """

    def __init__(self) -> None:
        self.__state: MutableMapping[int, TaskBuilderMode] = {}

    def get_final_mode(self, transition_mode: TaskBuilderMode) -> TaskBuilderMode:
        return (
            TaskBuilderMode.IMMEDIATE
            if transition_mode == TaskBuilderMode.TRANSITION_IMMEDIATE
            else TaskBuilderMode.JITTERED
        )

    def get_start_mode(self, transition_mode: TaskBuilderMode) -> TaskBuilderMode:
        return (
            TaskBuilderMode.IMMEDIATE
            if transition_mode == TaskBuilderMode.TRANSITION_JITTER
            else TaskBuilderMode.JITTERED
        )

    def get_current_mode(
        self, subscription: Subscription, timestamp: int
    ) -> TaskBuilderMode:
        general_mode = TaskBuilderMode(
            state.get_config(
                "subscription_primary_task_builder", TaskBuilderMode.JITTERED
            )
        )

        if (
            general_mode == TaskBuilderMode.IMMEDIATE
            or general_mode == TaskBuilderMode.JITTERED
        ):
            return general_mode

        resolution = int(subscription.data.resolution.total_seconds())
        if resolution > settings.MAX_RESOLUTION_FOR_JITTER:
            return self.get_final_mode(general_mode)

        if timestamp % resolution == 0:
            self.__state[resolution] = self.get_final_mode(general_mode)

        current_state = self.__state.get(resolution)
        return (
            current_state
            if current_state is not None
            else self.get_start_mode(general_mode)
        )


class DelegateTaskBuilder(TaskBuilder):
    """
    A delegate capable of switching back and forth between the
    immediate and jittered task builders according to runtime
    settings.

    It relies on TaskBuilderModeState to decide which task builder
    to use. The reason for this is that we cannot simply switch
    from one mode to another at any point in time. We need to wait
    for the end of the resolution time interval, or we risk to
    skip some queries.

    Example: if we transitioned from jittered to immediate at the
    second 30 of a minute. A query scheduled with a jitter = 40
    would not be scheduled at all during that minute because,
    with the immediate task builder, that would be scheduled at
    second 0.
    """

    def __init__(self) -> None:
        self.__immediate_builder = ImmediateTaskBuilder()
        self.__jittered_builder = JitteredTaskBuilder()
        self.__rollout_state = TaskBuilderModeState()

    def get_task(
        self, subscription_with_metadata: SubscriptionWithMetadata, timestamp: int
    ) -> Optional[ScheduledSubscriptionTask]:
        subscription = subscription_with_metadata.subscription

        primary_builder = self.__rollout_state.get_current_mode(subscription, timestamp)

        if primary_builder == TaskBuilderMode.JITTERED:
            return self.__jittered_builder.get_task(
                subscription_with_metadata, timestamp
            )
        else:
            return self.__immediate_builder.get_task(
                subscription_with_metadata, timestamp
            )

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


class SubscriptionScheduler(SubscriptionSchedulerBase):
    def __init__(
        self,
        entity_key: EntityKey,
        store: SubscriptionDataStore,
        partition_id: PartitionId,
        cache_ttl: timedelta,
        metrics: MetricsBackend,
    ) -> None:
        self.__entity_key = entity_key
        self.__store = store
        self.__cache_ttl = cache_ttl
        self.__partition_id = partition_id
        self.__metrics = metrics

        self.__subscriptions: List[Subscription] = []
        self.__last_refresh: Optional[datetime] = None

        self.__delegate_builder = DelegateTaskBuilder()
        self.__jittered_builder = JitteredTaskBuilder()
        self.__immediate_builder = ImmediateTaskBuilder()

        self.__reset_builder()

    def __reset_builder(self) -> None:
        """
        Use the jittered or immediate builder directly if we can as it is faster.
        If we are in transition between the two modes, we must use the delegate builder.
        This function is called for every tick.
        """
        general_mode = TaskBuilderMode(
            state.get_config(
                "subscription_primary_task_builder", TaskBuilderMode.JITTERED
            )
        )
        if general_mode == TaskBuilderMode.JITTERED:
            self.__builder: TaskBuilder = self.__jittered_builder
        elif general_mode == TaskBuilderMode.IMMEDIATE:
            self.__builder = self.__immediate_builder
        else:
            # We are transitioning between jittered and immediate mode. We must use the delegate builder.
            self.__builder = self.__delegate_builder

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

    def find(self, tick: Tick) -> Iterator[ScheduledSubscriptionTask]:
        self.__reset_builder()
        start_get_subscriptions = datetime.now()

        interval = tick.timestamps

        subscriptions = self.__get_subscriptions()

        self.__metrics.timing(
            "getting_subscriptions",
            (datetime.now() - start_get_subscriptions).total_seconds() * 1000.0,
        )

        start_get_task = datetime.now()

        for timestamp in range(
            math.ceil(interval.lower.timestamp()),
            math.ceil(interval.upper.timestamp()),
        ):
            for subscription in subscriptions:
                task = self.__builder.get_task(
                    SubscriptionWithMetadata(
                        self.__entity_key, subscription, tick.offsets.upper
                    ),
                    timestamp,
                )
                if task is not None:
                    yield task

        self.__metrics.timing(
            "getting_tasks", (datetime.now() - start_get_task).total_seconds() * 1000.0
        )

        start_reset_metrics = datetime.now()

        metrics = self.__builder.reset_metrics()
        if any(metric for metric in metrics if metric[1] > 0):
            for metric in metrics:
                self.__metrics.increment(metric[0], metric[1], tags=metric[2])

        self.__metrics.timing(
            "updating_metrics",
            (datetime.now() - start_reset_metrics).total_seconds() * 1000,
        )
