from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from typing import Iterable, MutableMapping, Tuple
from uuid import UUID, uuid1

from snuba.datasets.factory import get_dataset
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
)
from snuba.utils.metrics.backends.dummy import DummyMetricsBackend
from snuba.utils.streams import Message, Partition, Topic
from snuba.utils.streams.backends.local.backend import LocalBroker as Broker
from snuba.utils.types import Interval


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


def test_subscription_worker(broker: Broker[SubscriptionTaskResult],) -> None:
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

    dataset = get_dataset("events")
    worker = SubscriptionWorker(
        dataset,
        ThreadPoolExecutor(),
        {0: SubscriptionScheduler(store, PartitionId(0), timedelta(), metrics)},
        broker.get_producer(),
        result_topic,
        metrics,
    )

    now = datetime(2000, 1, 1)

    tick = Tick(
        offsets=Interval(0, 1),
        timestamps=Interval(now - (frequency * evaluations), now),
    )

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

        task, future = result_futures[i]
        future_result = request, result = future.result()
        assert message.payload.task.timestamp == timestamp
        assert message.payload == SubscriptionTaskResult(task, future_result)

        # NOTE: The time series extension is folded back into the request
        # body, ideally this would reference the timeseries options in
        # isolation.
        assert (
            request.body.items()
            > {
                "from_date": (timestamp - subscription.data.time_window).isoformat(),
                "to_date": timestamp.isoformat(),
            }.items()
        )

        assert result == {
            "meta": [{"name": "count", "type": "UInt64"}],
            "data": [{"count": 0}],
        }
