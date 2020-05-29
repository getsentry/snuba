from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from typing import Iterable, Iterator, MutableMapping, Tuple
from uuid import UUID, uuid1

import pytest

from snuba.datasets.dataset import Dataset
from snuba.subscriptions.consumer import Tick
from snuba.subscriptions.data import (
    PartitionId,
    Subscription,
    SubscriptionData,
    SubscriptionIdentifier,
)
from snuba.subscriptions.scheduler import SubscriptionScheduler
from snuba.subscriptions.store import SubscriptionDataStore
from snuba.subscriptions.worker import SubscriptionTaskResult, SubscriptionWorker
from snuba.utils.metrics.backends.dummy import DummyMetricsBackend
from snuba.utils.streams.consumer import Consumer
from snuba.utils.streams.dummy import DummyBroker, DummyConsumer, DummyProducer
from snuba.utils.streams.types import Message, Partition, Topic
from snuba.utils.types import Interval
from tests.base import dataset_manager


class DummySubscriptionDataStore(SubscriptionDataStore):
    def __init__(self) -> None:
        self.__subscriptions: MutableMapping[UUID, SubscriptionData] = {}

    def create(self, key: UUID, data: SubscriptionData) -> None:
        self.__subscriptions[key] = data

    def delete(self, key: UUID) -> None:
        try:
            del self.__subscriptions[key]
        except KeyError:
            pass

    def all(self) -> Iterable[Tuple[UUID, SubscriptionData]]:
        return [*self.__subscriptions.items()]


@pytest.fixture
def dataset() -> Iterator[Dataset]:
    with dataset_manager("events") as dataset:
        yield dataset


def test_subscription_worker(dataset: Dataset) -> None:
    result_topic = Topic("subscription-results")

    broker: DummyBroker[SubscriptionTaskResult] = DummyBroker()
    broker.create_topic(result_topic, partitions=1)

    frequency = timedelta(minutes=1)
    evaluations = 3

    subscription = Subscription(
        SubscriptionIdentifier(PartitionId(0), uuid1()),
        SubscriptionData(
            project_id=1,
            conditions=[],
            aggregations=[["count()", "", "count"]],
            time_window=timedelta(minutes=60),
            resolution=frequency,
        ),
    )

    store = DummySubscriptionDataStore()
    store.create(subscription.identifier.uuid, subscription.data)

    metrics = DummyMetricsBackend(strict=True)

    worker = SubscriptionWorker(
        dataset,
        ThreadPoolExecutor(),
        {0: SubscriptionScheduler(store, PartitionId(0), timedelta(), metrics)},
        DummyProducer(broker),
        result_topic,
        metrics,
    )

    now = datetime(2000, 1, 1)

    results = worker.process_message(
        Message(
            Partition(Topic("events"), 0),
            0,
            Tick(
                offsets=Interval(0, 1),
                timestamps=Interval(now - (frequency * evaluations), now),
            ),
            now,
        )
    )

    assert results is not None and len(results) == evaluations

    # Publish the results.
    worker.flush_batch([results])

    # Check to make sure the results were published.
    # NOTE: This does not cover the ``SubscriptionTaskResultCodec``!
    consumer: Consumer[SubscriptionTaskResult] = DummyConsumer(broker, "group")
    consumer.subscribe([result_topic])

    for i in range(evaluations):
        timestamp = now - frequency * (evaluations - i)

        message = consumer.poll()
        assert message is not None
        assert message.partition.topic == result_topic
        assert message.payload.task == results[i].task
        assert message.payload.task.timestamp == timestamp
        assert message.payload.result == results[i].future.result()
        request, result = message.payload.result
        assert request.extensions["timeseries"] == {
            "from_date": (timestamp - subscription.data.time_window).isoformat(),
            "to_date": timestamp.isoformat(),
            "granularity": 3600,  # XXX: unused, no time grouping
        }
        assert result == {
            "meta": [{"name": "count", "type": "UInt64"}],
            "data": [{"count": 0}],
        }
