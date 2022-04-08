import uuid
from datetime import datetime

from arroyo import Topic
from arroyo.backends.kafka import KafkaPayload
from arroyo.backends.local.backend import LocalBroker
from arroyo.backends.local.storages.memory import MemoryMessageStorage
from arroyo.utils.clock import TestingClock

from snuba.datasets.entities import EntityKey
from snuba.datasets.factory import get_dataset
from snuba.reader import Result
from snuba.subscriptions.codecs import SubscriptionTaskResultEncoder
from snuba.subscriptions.data import (
    PartitionId,
    ScheduledSubscriptionTask,
    Subscription,
    SubscriptionData,
    SubscriptionIdentifier,
    SubscriptionTaskResult,
    SubscriptionWithMetadata,
)
from snuba.subscriptions.entity_subscription import EventsSubscription
from snuba.subscriptions.verifier import (
    ResultStore,
    ResultTopic,
    SubscriptionResultConsumer,
    SubscriptionResultData,
    SubscriptionResultDecoder,
)
from snuba.utils.metrics.timer import Timer
from tests.backends.metrics import Increment, TestingMetricsBackend


def make_encoded_subscription_result(
    subscription_identifier: SubscriptionIdentifier, timestamp: datetime
) -> KafkaPayload:
    encoder = SubscriptionTaskResultEncoder()

    entity_subscription = EventsSubscription(data_dict={})
    subscription_data = SubscriptionData(
        project_id=1,
        query="MATCH (events) SELECT count() AS count",
        time_window_sec=60,
        resolution_sec=60,
        entity_subscription=entity_subscription,
    )

    request = subscription_data.build_request(
        get_dataset("events"), timestamp, None, Timer("timer")
    )
    result: Result = {
        "meta": [{"type": "UInt64", "name": "count"}],
        "data": [{"count": 1}],
    }

    task_result = SubscriptionTaskResult(
        ScheduledSubscriptionTask(
            timestamp,
            SubscriptionWithMetadata(
                EntityKey.EVENTS,
                Subscription(subscription_identifier, subscription_data,),
                5,
            ),
        ),
        (request, result),
    )

    encoded_message = encoder.encode(task_result)
    return encoded_message


def test_decoder() -> None:
    subscription_identifier = SubscriptionIdentifier(PartitionId(1), uuid.uuid1())
    timestamp = datetime.now()

    encoded_message = make_encoded_subscription_result(
        subscription_identifier, timestamp
    )

    decoder = SubscriptionResultDecoder()

    decoded = decoder.decode(encoded_message)

    assert decoded.subscription_id == str(subscription_identifier)
    assert decoded.timestamp == int(timestamp.timestamp())
    assert decoded.entity_name == "events"
    assert decoded.result == {
        "meta": [{"type": "UInt64", "name": "count"}],
        "data": [{"count": 1}],
    }


def test_subscription_result_consumer() -> None:
    clock = TestingClock()
    broker: LocalBroker[KafkaPayload] = LocalBroker(MemoryMessageStorage(), clock)
    group_id = "test_group"
    topic_orig = Topic("result_topic_one")
    topic_new = Topic("result_topic_two")
    broker.create_topic(topic_orig, partitions=2)
    broker.create_topic(topic_new, partitions=2)
    inner_consumer = broker.get_consumer(group_id)
    consumer = SubscriptionResultConsumer(
        inner_consumer, override_topics=[topic_orig, topic_new]
    )
    consumer.subscribe([topic_orig])  # Should be ignored
    assert consumer.poll() is None
    producer = broker.get_producer()

    subscription_identifier = SubscriptionIdentifier(PartitionId(1), uuid.uuid1())
    timestamp = datetime.now()

    encoded_message = make_encoded_subscription_result(
        subscription_identifier, timestamp
    )

    producer.produce(topic_orig, encoded_message)
    producer.produce(topic_new, encoded_message)

    # First message
    message = consumer.poll()
    assert message is not None
    assert message.payload.entity_name == "events"
    assert message.payload.subscription_id == str(subscription_identifier)
    assert message.payload.timestamp == int(timestamp.timestamp())
    assert message.payload.result == {
        "meta": [{"type": "UInt64", "name": "count"}],
        "data": [{"count": 1}],
    }
    assert message.partition.topic == topic_orig

    # Second message
    message = consumer.poll()
    assert message is not None
    assert message.payload.entity_name == "events"
    assert message.payload.subscription_id == str(subscription_identifier)
    assert message.payload.timestamp == int(timestamp.timestamp())
    assert message.payload.result == {
        "meta": [{"type": "UInt64", "name": "count"}],
        "data": [{"count": 1}],
    }
    assert message.partition.topic == topic_new


def test_result_store() -> None:
    metrics = TestingMetricsBackend()
    store = ResultStore(threshold_sec=2, metrics=metrics)
    now = 5

    def get_result_data(timestamp: int) -> SubscriptionResultData:
        return SubscriptionResultData(
            "some-id",
            {},
            {"meta": [], "data": [], "totals": {}, "trace_output": "idk"},
            timestamp,
            "events",
        )

    store.increment(ResultTopic.ORIGINAL, get_result_data(now))
    assert len(metrics.calls) == 0  # Nothing to record yet

    # Advance 1 second, it's below the low watermark and will be discarded
    store.increment(ResultTopic.ORIGINAL, get_result_data(now + 1))
    assert len(metrics.calls) == 0

    # Advance 2 seconds
    store.increment(ResultTopic.ORIGINAL, get_result_data(now + 3))
    assert len(metrics.calls) == 0

    # Now we can record the now + 3 metrics
    store.increment(ResultTopic.ORIGINAL, get_result_data(now + 5))
    assert len(metrics.calls) == 1
    assert Increment("result_count_diff", 1, None) in metrics.calls
    metrics.calls = []

    # We get a message in the new topic
    store.increment(ResultTopic.NEW, get_result_data(now + 5))
    assert len(metrics.calls) == 0

    # The diff for now + 5 is 0 since we got 1 message in each topic
    store.increment(ResultTopic.ORIGINAL, get_result_data(now + 10))
    assert len(metrics.calls) == 1
    assert Increment("result_count_diff", 0, None) in metrics.calls
    metrics.calls = []

    # Stale message
    store.increment(ResultTopic.ORIGINAL, get_result_data(now))
    assert len(metrics.calls) == 1
    assert Increment("stale_message", 1, None) in metrics.calls
