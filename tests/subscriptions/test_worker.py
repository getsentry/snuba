import itertools
import string
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from random import Random
from typing import Any, Iterable, Iterator, Mapping, Type
from uuid import UUID, uuid1

import pytest

from snuba import settings
from snuba.datasets.dataset import Dataset
from snuba.datasets.factory import enforce_table_writer, get_dataset_name
from snuba.processor import ProcessorAction
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
from tests.subscriptions.fixtures import dataset  # NOQA


@pytest.fixture
def random() -> Random:
    return Random()


Payload = Any  # XXX: yuck


class PayloadBuilder(ABC):
    def __init__(self, random: Random) -> None:
        self.random = random

    @abstractmethod
    def build(self, project_id: int, timestamp: datetime) -> Payload:
        raise NotImplementedError


class EventPayloadBuilder(PayloadBuilder):
    def build(self, project_id: int, timestamp: datetime) -> Payload:
        scale = 1
        return [
            2,
            "insert",
            {
                "event_id": UUID(int=self.random.randint(0, 2 ** 128)).hex,
                "project_id": project_id,
                "group_id": (
                    ((project_id - 1) * (10 * scale))
                    + self.random.randint(0, 10 * scale)
                ),
                "datetime": timestamp.strftime(settings.PAYLOAD_DATETIME_FORMAT),
                "platform": self.random.choice(["python", "javascript"]),
                "primary_hash": UUID(int=self.random.randint(0, 2 ** 128)).hex,
                "message": "".join(
                    self.random.choice(string.printable)
                    for i in range(self.random.randint(10, 255))
                ),
                "data": {
                    "received": (
                        timestamp - timedelta(seconds=int(self.random.random() * 30))
                    ).timestamp()
                },
            },
            {},  # XXX
        ]


class TransactionPayloadBuilder(PayloadBuilder):
    def build(self, project_id: int, timestamp: datetime) -> Payload:
        return [
            2,
            "insert",
            {
                "event_id": UUID(int=self.random.randint(0, 2 ** 128)).hex,
                "project_id": project_id,
                "platform": self.random.choice(["python", "javascript"]),
                "data": {
                    "type": "transaction",
                    "start_timestamp": (
                        timestamp - timedelta(seconds=self.random.random() * 300)
                    ).timestamp(),
                    "timestamp": timestamp.timestamp(),  # WHY
                    "contexts": {
                        "trace": {
                            "trace_id": UUID(int=self.random.randint(0, 2 ** 128)).hex,
                            "span_id": f"{self.random.randint(0, 2 ** 64):x}",
                        }
                    },
                },
            },
            {},  # XXX
        ]


builders: Mapping[str, Type[PayloadBuilder]] = {
    "events": EventPayloadBuilder,
    "transactions": TransactionPayloadBuilder,
}


@pytest.fixture
def builder(random: Random, dataset: Dataset) -> PayloadBuilder:
    builder_type = builders[get_dataset_name(dataset)]
    return builder_type(random)


class DummyScheduler(Scheduler[Subscription]):
    def __init__(self, tasks: Iterable[ScheduledTask[Subscription]]):
        self.__tasks = tasks

    def find(
        self, interval: Interval[datetime]
    ) -> Iterator[ScheduledTask[Subscription]]:
        for task in self.__tasks:
            yield task


def test_worker(random: Random, dataset: Dataset, builder: PayloadBuilder) -> None:
    project_id = random.randint(1, 2 ** 32)
    minutes = 20  # XXX?
    start_time = datetime.utcnow().replace(
        minute=0, second=0, microsecond=0
    ) - timedelta(minutes=minutes)

    payloads = [
        builder.build(project_id, timestamp)
        for timestamp in (start_time + timedelta(minutes=i) for i in range(minutes))
    ]

    table_writer = enforce_table_writer(dataset)
    event_processor = table_writer.get_stream_loader().get_processor()

    # XXX: This is pretty clunky...
    rows = [
        message.data
        for message in [
            event_processor.process_message(payload) for payload in payloads
        ]
        if message is not None and message.action is ProcessorAction.INSERT
    ]
    assert len(rows) == len(payloads)  # sanity check

    batch_writer = table_writer.get_writer()
    batch_writer.write(itertools.chain.from_iterable(rows))

    result_topic = Topic("subscription-results")

    broker: DummyBroker[SubscriptionTaskResult] = DummyBroker()
    broker.create_topic(result_topic, partitions=1)

    subscription = Subscription(
        SubscriptionIdentifier(PartitionId(0), uuid1()),
        SubscriptionData(
            project_id=project_id,
            conditions=[],
            aggregations=[["count()", "", "count"]],
            time_window=timedelta(minutes=500),
            resolution=timedelta(minutes=1),
        ),
    )

    now = datetime.utcnow()

    timestamps = [
        now,
        (now + timedelta(minutes=minutes) + subscription.data.time_window),
    ]

    scheduler = DummyScheduler(
        ScheduledTask(timestamp, subscription) for timestamp in timestamps
    )

    worker = SubscriptionWorker(
        dataset,
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
        "data": [{"count": 20}],
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
