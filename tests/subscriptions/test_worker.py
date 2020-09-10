import json
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from typing import Iterable, Iterator, MutableMapping, Optional, Tuple
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
from snuba.subscriptions.worker import (
    SubscriptionWorker,
    SubscriptionTaskResult,
    subscription_task_result_encoder,
)
from snuba.utils.metrics.backends.dummy import DummyMetricsBackend
from snuba.utils.streams import Message, Partition, Topic
from snuba.utils.streams.backends.dummy import DummyBroker
from snuba.utils.streams.backends.kafka import KafkaPayload
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


@pytest.mark.parametrize(
    "time_shift",
    [
        pytest.param(None, id="without time shift"),
        pytest.param(timedelta(minutes=-5), id="with time shift"),
    ],
)
def test_subscription_worker(
    dataset: Dataset, broker: DummyBroker[KafkaPayload], time_shift: Optional[timedelta]
) -> None:
    result_topic = Topic("subscription-results")

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
        broker.get_producer(),
        result_topic,
        metrics,
        time_shift=time_shift,
    )

    now = datetime(2000, 1, 1)

    tick = Tick(
        offsets=Interval(0, 1),
        timestamps=Interval(now - (frequency * evaluations), now),
    )

    # If we are utilizing time shifting, push the tick time into the future so
    # that time alignment is otherwise preserved during our test case.
    if time_shift is not None:
        tick = tick.time_shift(time_shift * -1)

    result_futures = worker.process_message(
        Message(Partition(Topic("events"), 0), 0, tick, now)
    )

    assert result_futures is not None and len(result_futures) == evaluations

    # Publish the results.
    worker.flush_batch([result_futures])

    # Check to make sure the results were published.
    # NOTE: This does not cover the ``SubscriptionTaskResultCodec``!
    consumer = broker.get_consumer("group")
    consumer.subscribe([result_topic])

    for i in range(evaluations):
        timestamp = now - frequency * (evaluations - i)

        message = consumer.poll()
        assert message is not None
        assert message.partition.topic == result_topic

        task_result_future = result_futures[i]
        expected_payload = subscription_task_result_encoder.encode(
            SubscriptionTaskResult(
                task_result_future.task, task_result_future.future.result()
            )
        )

        assert message.payload == expected_payload

        decoded_payload = json.loads(expected_payload.value)["payload"]
        assert decoded_payload["timestamp"] == timestamp.isoformat()
        assert (
            # NOTE: The time series extension is folded back into the request
            # body, ideally this would reference the timeseries options in
            # isolation.
            decoded_payload["request"].items()
            > {
                "from_date": (timestamp - subscription.data.time_window).isoformat(),
                "to_date": timestamp.isoformat(),
            }.items()
        )
        assert decoded_payload["result"] == {
            "meta": [{"name": "count", "type": "UInt64"}],
            "data": [{"count": 0}],
        }
