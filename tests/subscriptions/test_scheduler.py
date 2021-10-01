import uuid
from datetime import datetime, timedelta
from typing import Callable, Collection, Optional, Tuple

from snuba import state
from snuba.datasets.entities import EntityKey
from snuba.redis import redis_client
from snuba.subscriptions.data import (
    LegacySubscriptionData,
    PartitionId,
    Subscription,
    SubscriptionIdentifier,
)
from snuba.subscriptions.scheduler import SubscriptionScheduler
from snuba.subscriptions.store import RedisSubscriptionDataStore
from snuba.utils.metrics.backends.dummy import DummyMetricsBackend
from snuba.utils.scheduler import ScheduledTask
from snuba.utils.types import Interval
from tests.subscriptions.subscriptions_utils import create_entity_subscription


class TestSubscriptionScheduler:
    def setup_method(self) -> None:
        self.now = datetime.utcnow().replace(minute=0, second=0, microsecond=0)
        self.partition_id = PartitionId(1)
        self.entity_key = EntityKey("events")

    def build_subscription(self, resolution: timedelta) -> Subscription:
        return Subscription(
            SubscriptionIdentifier(self.partition_id, uuid.uuid4()),
            LegacySubscriptionData(
                project_id=1,
                conditions=[],
                aggregations=[["count()", "", "count"]],
                time_window=timedelta(minutes=1),
                resolution=resolution,
                entity_subscription=create_entity_subscription(),
            ),
        )

    def build_interval(self, lower: timedelta, upper: timedelta) -> Interval[datetime]:
        return Interval(self.now + lower, self.now + upper)

    def sort_key(self, task: ScheduledTask[Subscription]) -> Tuple[datetime, uuid.UUID]:
        return task.timestamp, task.task.identifier.uuid

    def run_test(
        self,
        subscriptions: Collection[Subscription],
        start: timedelta,
        end: timedelta,
        expected: Collection[ScheduledTask[Subscription]],
        sort_key: Optional[
            Callable[[ScheduledTask[Subscription]], Tuple[datetime, uuid.UUID]]
        ] = None,
    ) -> None:
        store = RedisSubscriptionDataStore(
            redis_client, self.entity_key, self.partition_id,
        )
        for subscription in subscriptions:
            store.create(subscription.identifier.uuid, subscription.data)

        scheduler = SubscriptionScheduler(
            store,
            self.partition_id,
            timedelta(minutes=1),
            DummyMetricsBackend(strict=True),
        )

        result = list(scheduler.find(self.build_interval(start, end)))
        if sort_key:
            result.sort(key=sort_key)

        assert result == expected

    def test_simple(self) -> None:
        state.set_config("subscription_primary_task_builder", "immediate")
        subscription = self.build_subscription(timedelta(minutes=1))
        self.run_test(
            [subscription],
            start=timedelta(minutes=-10),
            end=timedelta(minutes=0),
            expected=[
                ScheduledTask(self.now + timedelta(minutes=-10 + i), subscription)
                for i in range(10)
            ],
        )

    def test_simple_jittered(self) -> None:
        subscription = self.build_subscription(timedelta(minutes=1))
        self.run_test(
            [subscription],
            start=timedelta(minutes=-10),
            end=timedelta(minutes=0),
            expected=[
                ScheduledTask(self.now + timedelta(minutes=-10 + i), subscription)
                for i in range(10)
            ],
        )

    def test_subscription_not_running(self) -> None:
        self.run_test(
            [self.build_subscription(timedelta(minutes=3))],
            start=timedelta(minutes=-2),
            end=timedelta(minutes=0),
            expected=[],
        )

    def test_subscription_resolution_larger_than_interval(self) -> None:
        subscription = self.build_subscription(timedelta(minutes=3))
        self.run_test(
            [subscription],
            start=timedelta(minutes=-1),
            end=timedelta(minutes=1),
            expected=[ScheduledTask(self.now, subscription)],
        )

    def test_subscription_resolution_larger_than_tiny_interval(self) -> None:
        state.set_config("subscription_primary_task_builder", "immediate")
        subscription = self.build_subscription(timedelta(minutes=1))
        self.run_test(
            [subscription],
            start=timedelta(seconds=-1),
            end=timedelta(seconds=1),
            expected=[ScheduledTask(self.now, subscription)],
        )

    def test_multiple_subscriptions(self) -> None:
        subscription = self.build_subscription(timedelta(minutes=1))
        other_subscription = self.build_subscription(timedelta(minutes=2))
        expected = [
            ScheduledTask(self.now + timedelta(minutes=-10 + i), subscription)
            for i in range(10)
        ] + [
            ScheduledTask(self.now + timedelta(minutes=-10 + i), other_subscription)
            for i in range(0, 10, 2)
        ]
        expected.sort(key=self.sort_key)
        self.run_test(
            [subscription, other_subscription],
            start=timedelta(minutes=-10),
            end=timedelta(minutes=0),
            expected=expected,
            sort_key=self.sort_key,
        )
