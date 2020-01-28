from datetime import datetime, timedelta
from unittest.mock import Mock
from uuid import uuid1

from concurrent.futures import ThreadPoolExecutor

from snuba import settings
from snuba.subscriptions.consumer import Tick
from snuba.subscriptions.data import (
    PartitionId,
    Subscription,
    SubscriptionData,
    SubscriptionIdentifier,
)
from snuba.subscriptions.executor import SubscriptionExecutor
from snuba.subscriptions.scheduler import ScheduledTask
from snuba.utils.types import Interval
from tests.subscriptions import BaseSubscriptionTest


class TestSubscriptionExecutor(BaseSubscriptionTest):
    def test(self):
        executor = SubscriptionExecutor(
            self.dataset,
            ThreadPoolExecutor(
                max_workers=settings.SUBSCRIPTIONS_MAX_CONCURRENT_QUERIES
            ),
        )
        subscription = Subscription(
            SubscriptionIdentifier(PartitionId(0), uuid1()),
            SubscriptionData(
                project_id=self.project_id,
                conditions=[["platform", "IN", ["a"]]],
                aggregations=[["count()", "", "count"]],
                time_window=timedelta(minutes=500),
                resolution=timedelta(minutes=1),
            ),
        )
        now = datetime.utcnow()
        task = ScheduledTask(now, subscription)
        tick = Tick(
            offsets=Interval(1, 2),
            timestamps=Interval(now - timedelta(minutes=1), now),
        )

        future = executor.execute(task, tick, Mock())
        result = future.result()

        assert result["data"][0]["count"] == 10

        tick = Tick(
            offsets=Interval(5000, 5001),
            timestamps=Interval(now + timedelta(hours=10), now + timedelta(hours=11)),
        )
        future = executor.execute(task, tick, Mock())
        result = future.result()

        assert result["data"][0]["count"] == 0
