import uuid
from datetime import datetime, timedelta
from typing import Callable, Collection, Optional, Tuple

import pytest

from snuba import state
from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.redis import RedisClientKey, get_redis_client
from snuba.subscriptions.data import (
    PartitionId,
    ScheduledSubscriptionTask,
    Subscription,
    SubscriptionData,
    SubscriptionIdentifier,
    SubscriptionWithMetadata,
)
from snuba.subscriptions.scheduler import SubscriptionScheduler
from snuba.subscriptions.store import RedisSubscriptionDataStore
from snuba.subscriptions.types import Interval
from snuba.subscriptions.utils import Tick
from snuba.utils.metrics.backends.dummy import DummyMetricsBackend

redis_client = get_redis_client(RedisClientKey.SUBSCRIPTION_STORE)


class TestSubscriptionScheduler:
    def setup_method(self) -> None:
        self.now = datetime.utcnow().replace(minute=0, second=0, microsecond=0)
        self.partition_id = PartitionId(1)
        self.entity_key = EntityKey("events")

    def build_subscription(self, resolution: timedelta) -> Subscription:
        return Subscription(
            SubscriptionIdentifier(self.partition_id, uuid.uuid4()),
            SubscriptionData(
                project_id=1,
                query="MATCH (events) SELECT count() AS count",
                time_window_sec=60,
                resolution_sec=int(resolution.total_seconds()),
                entity=get_entity(EntityKey.EVENTS),
                metadata={},
            ),
        )

    def build_tick(self, lower: timedelta, upper: timedelta) -> Tick:
        return Tick(1, Interval(1, 5), Interval(self.now + lower, self.now + upper))

    def sort_key(self, task: ScheduledSubscriptionTask) -> Tuple[datetime, uuid.UUID]:
        return task.timestamp, task.task.subscription.identifier.uuid

    def run_test(
        self,
        subscriptions: Collection[Subscription],
        start: timedelta,
        end: timedelta,
        expected: Collection[ScheduledSubscriptionTask],
        sort_key: Optional[
            Callable[[ScheduledSubscriptionTask], Tuple[datetime, uuid.UUID]]
        ] = None,
    ) -> None:
        tick = self.build_tick(start, end)

        store = RedisSubscriptionDataStore(
            redis_client,
            self.entity_key,
            self.partition_id,
        )
        for subscription in subscriptions:
            store.create(subscription.identifier.uuid, subscription.data)

        scheduler = SubscriptionScheduler(
            EntityKey.EVENTS,
            store,
            self.partition_id,
            timedelta(minutes=1),
            DummyMetricsBackend(strict=True),
        )

        result = list(scheduler.find(tick))
        if sort_key:
            result.sort(key=sort_key)

        assert result == expected

    @pytest.mark.redis_db
    def test_simple(self) -> None:
        state.set_config("subscription_primary_task_builder", "immediate")
        subscription = self.build_subscription(timedelta(minutes=1))
        start = timedelta(minutes=-10)
        end = timedelta(minutes=0)
        self.run_test(
            [subscription],
            start=start,
            end=end,
            expected=[
                ScheduledSubscriptionTask(
                    self.now + timedelta(minutes=-10 + i),
                    SubscriptionWithMetadata(
                        EntityKey.EVENTS,
                        subscription,
                        self.build_tick(start, end).offsets.upper,
                    ),
                )
                for i in range(10)
            ],
        )

    @pytest.mark.redis_db
    def test_simple_jittered(self) -> None:
        subscription = self.build_subscription(timedelta(minutes=1))
        start = timedelta(minutes=-10)
        end = timedelta(minutes=0)

        self.run_test(
            [subscription],
            start=start,
            end=end,
            expected=[
                ScheduledSubscriptionTask(
                    self.now + timedelta(minutes=-10 + i),
                    SubscriptionWithMetadata(
                        EntityKey.EVENTS,
                        subscription,
                        self.build_tick(start, end).offsets.upper,
                    ),
                )
                for i in range(10)
            ],
        )

    @pytest.mark.redis_db
    def test_subscription_not_running(self) -> None:
        self.run_test(
            [self.build_subscription(timedelta(minutes=3))],
            start=timedelta(minutes=-2),
            end=timedelta(minutes=0),
            expected=[],
        )

    @pytest.mark.redis_db
    def test_subscription_resolution_larger_than_interval(self) -> None:
        subscription = self.build_subscription(timedelta(minutes=3))
        start = timedelta(minutes=-1)
        end = timedelta(minutes=1)
        self.run_test(
            [subscription],
            start=start,
            end=end,
            expected=[
                ScheduledSubscriptionTask(
                    self.now,
                    SubscriptionWithMetadata(
                        EntityKey.EVENTS,
                        subscription,
                        self.build_tick(start, end).offsets.upper,
                    ),
                )
            ],
        )

    @pytest.mark.redis_db
    def test_subscription_resolution_larger_than_tiny_interval(self) -> None:
        state.set_config("subscription_primary_task_builder", "immediate")
        subscription = self.build_subscription(timedelta(minutes=1))
        start = timedelta(seconds=-1)
        end = timedelta(seconds=1)
        self.run_test(
            [subscription],
            start=start,
            end=end,
            expected=[
                ScheduledSubscriptionTask(
                    self.now,
                    SubscriptionWithMetadata(
                        EntityKey.EVENTS,
                        subscription,
                        self.build_tick(start, end).offsets.upper,
                    ),
                )
            ],
        )

    @pytest.mark.redis_db
    def test_multiple_subscriptions(self) -> None:
        subscription = self.build_subscription(timedelta(minutes=1))
        other_subscription = self.build_subscription(timedelta(minutes=2))
        start = timedelta(minutes=-10)
        end = timedelta(minutes=0)
        expected = [
            ScheduledSubscriptionTask(
                self.now + timedelta(minutes=-10 + i),
                SubscriptionWithMetadata(
                    EntityKey.EVENTS,
                    subscription,
                    self.build_tick(start, end).offsets.upper,
                ),
            )
            for i in range(10)
        ] + [
            ScheduledSubscriptionTask(
                self.now + timedelta(minutes=-10 + i),
                SubscriptionWithMetadata(
                    EntityKey.EVENTS,
                    other_subscription,
                    self.build_tick(start, end).offsets.upper,
                ),
            )
            for i in range(0, 10, 2)
        ]

        expected.sort(key=self.sort_key)
        self.run_test(
            [subscription, other_subscription],
            start=start,
            end=end,
            expected=expected,
            sort_key=self.sort_key,
        )
