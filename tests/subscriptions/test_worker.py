from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from typing import Iterable, Iterator
from uuid import uuid1

from snuba.subscriptions.consumer import Tick
from snuba.subscriptions.data import (
    PartitionId,
    Subscription,
    SubscriptionData,
    SubscriptionIdentifier,
)
from snuba.subscriptions.scheduler import ScheduledTask, Scheduler
from snuba.subscriptions.worker import SubscriptionTaskResult, SubscriptionWorker
from snuba.utils.metrics.backends.dummy import DummyMetricsBackend
from snuba.utils.streams.consumer import Consumer
from snuba.utils.streams.dummy import DummyBroker, DummyConsumer, DummyProducer
from snuba.utils.streams.types import Message, Partition, Topic
from snuba.utils.types import Interval
from tests.subscriptions import BaseSubscriptionTest


class DummyScheduler(Scheduler[Subscription]):
    def __init__(self, tasks: Iterable[ScheduledTask[Subscription]]):
        self.__tasks = tasks

    def find(
        self, interval: Interval[datetime]
    ) -> Iterator[ScheduledTask[Subscription]]:
        for task in self.__tasks:
            yield task


class TestSubscriptionWorker(BaseSubscriptionTest):
    def test(self):
        result_topic = Topic("subscription-results")

        broker: DummyBroker[SubscriptionTaskResult] = DummyBroker()
        broker.create_topic(result_topic, partitions=1)

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

        timestamps = [
            now,
            (now + timedelta(minutes=self.minutes) + subscription.data.time_window),
        ]

        scheduler = DummyScheduler(
            ScheduledTask(timestamp, subscription) for timestamp in timestamps
        )

        worker = SubscriptionWorker(
            self.dataset,
            ThreadPoolExecutor(),
            {0: scheduler},
            DummyProducer(broker),
            result_topic,
            DummyMetricsBackend(strict=True),
        )

        # Process the tick message and run the scheduled queries.
        results = worker.process_message(
            Message(
                Partition(Topic("events"), 0),
                0,
                Tick(  # XXX: isn't actually used in the dummy scheduler
                    offsets=Interval(0, 1),
                    timestamps=Interval(timestamps[0], timestamps[-1]),
                ),
                timestamps[-1],
            ),
        )

        assert results is not None

        # Publish the results.
        worker.flush_batch([results])

        # Check to make sure the results were published.
        # NOTE: This does not cover the ``SubscriptionTaskResultCodec``!
        consumer: Consumer[SubscriptionTaskResult] = DummyConsumer(broker, "group")
        consumer.subscribe([result_topic])

        message = consumer.poll()
        assert message is not None
        assert message.partition.topic == result_topic
        assert message.payload.task == results[0].task
        assert message.payload.task.timestamp == timestamps[0]
        assert message.payload.result == results[0].future.result()
        request, result = message.payload.result
        assert request.extensions["timeseries"] == {
            "from_date": (timestamps[0] - subscription.data.time_window).isoformat(),
            "to_date": timestamps[0].isoformat(),
            "granularity": 3600,  # XXX: unused, no time grouping
        }
        assert result == {
            "meta": [{"name": "count", "type": "UInt64"}],
            "data": [{"count": 10}],
        }

        message = consumer.poll()
        assert message is not None
        assert message.partition.topic == result_topic
        assert message.payload.task == results[1].task
        assert message.payload.task.timestamp == timestamps[1]
        assert message.payload.result == results[1].future.result()
        request, result = message.payload.result
        assert request.extensions["timeseries"] == {
            "from_date": (timestamps[1] - subscription.data.time_window).isoformat(),
            "to_date": timestamps[1].isoformat(),
            "granularity": 3600,  # XXX: unused, no time grouping
        }
        assert result == {
            "meta": [{"name": "count", "type": "UInt64"}],
            "data": [{"count": 0}],
        }
