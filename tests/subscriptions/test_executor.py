from datetime import datetime, timedelta
from uuid import uuid1

from concurrent.futures import ThreadPoolExecutor

from snuba.subscriptions.consumer import Tick
from snuba.subscriptions.data import (
    PartitionId,
    Subscription,
    SubscriptionData,
    SubscriptionIdentifier,
)
from snuba.subscriptions.executor import SubscriptionExecutor
from snuba.subscriptions.scheduler import ScheduledTask
from snuba.utils.metrics.backends.dummy import DummyMetricsBackend
from snuba.utils.types import Interval
from tests.subscriptions import BaseSubscriptionTest


class TestSubscriptionExecutor(BaseSubscriptionTest):
    def test(self):
        executor = SubscriptionExecutor(
            self.dataset, ThreadPoolExecutor(), DummyMetricsBackend(strict=True)
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
        tick = Tick(
            offsets=Interval(1, 2),
            timestamps=Interval(now - timedelta(minutes=1), now),
        )

        result = executor.execute(ScheduledTask(now, subscription), tick).result()
        assert result.result["data"][0]["count"] == 10

        result = executor.execute(
            ScheduledTask(
                now + timedelta(minutes=self.minutes) + subscription.data.time_window,
                subscription,
            ),
            tick,
        ).result()

        assert result.result["data"][0]["count"] == 0
