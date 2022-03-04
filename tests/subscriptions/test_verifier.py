import uuid
from datetime import datetime, timedelta
from random import randint

from arroyo import Topic
from arroyo.backends.abstract import Consumer
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
    SubscriptionResultConsumer,
    SubscriptionResultData,
    SubscriptionResultDecoder,
    SynchronizedConsumer,
)
from snuba.utils.metrics.timer import Timer


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
                Subscription(subscription_identifier, subscription_data),
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
    topic = Topic("result_topic")
    broker.create_topic(topic, partitions=2)
    inner_consumer = broker.get_consumer(group_id)
    consumer = SubscriptionResultConsumer(inner_consumer)
    consumer.subscribe([topic])
    assert consumer.poll() is None
    producer = broker.get_producer()

    subscription_identifier = SubscriptionIdentifier(PartitionId(1), uuid.uuid1())
    timestamp = datetime.now()

    encoded_message = make_encoded_subscription_result(
        subscription_identifier, timestamp
    )

    producer.produce(topic, encoded_message)
    message = consumer.poll()
    assert message is not None
    assert message.payload.entity_name == "events"
    assert message.payload.subscription_id == str(subscription_identifier)
    assert message.payload.timestamp == int(timestamp.timestamp())
    assert message.payload.result == {
        "meta": [{"type": "UInt64", "name": "count"}],
        "data": [{"count": 1}],
    }


def test_synchronized_consumer() -> None:
    orig_topic = Topic("old-result-topic")
    new_topic = Topic("new-result-topic")
    consumer_group = "verifier_group"
    auto_offset_reset = "latest"

    clock = TestingClock()
    broker: LocalBroker[KafkaPayload] = LocalBroker(MemoryMessageStorage(), clock)
    broker.create_topic(orig_topic, partitions=1)
    broker.create_topic(new_topic, partitions=1)
    producer = broker.get_producer()

    # Don't use actual Kafka
    class SychronizedConsumerWithFakeKafka(SynchronizedConsumer):
        def _build_result_consumer(
            self, consumer_group: str, auto_offset_reset: str
        ) -> Consumer[SubscriptionResultData]:
            # Use a randomly generated consumer group as the local consumer doesn't support using same group id
            group_id = f"{consumer_group}-{randint(0, 100000)}"
            fake_kafka_consumer = broker.get_consumer(group_id)
            return SubscriptionResultConsumer(fake_kafka_consumer)

    consumer = SychronizedConsumerWithFakeKafka(
        orig_topic, new_topic, consumer_group, auto_offset_reset
    )

    consumer.subscribe([])
    assert consumer.poll() is None

    now = datetime.now()

    # Produce to original topic
    for i in range(0, 3):
        subscription_identifier = SubscriptionIdentifier(PartitionId(0), uuid.uuid4())
        encoded_message = make_encoded_subscription_result(
            subscription_identifier, now + timedelta(seconds=2 * i)
        )
        producer.produce(orig_topic, encoded_message)

    # Produce to new topic
    for i in range(0, 3):
        subscription_identifier = SubscriptionIdentifier(PartitionId(0), uuid.uuid4())
        encoded_message = make_encoded_subscription_result(
            subscription_identifier, now + timedelta(seconds=5 * i)
        )
        producer.produce(new_topic, encoded_message)

    expected_order = [
        new_topic,
        orig_topic,
        orig_topic,
        orig_topic,
        new_topic,
        new_topic,
    ]

    for i in range(0, 6):
        message = consumer.poll()
        assert message is not None
        assert message.partition.topic == expected_order[i]

    # No more messages
    assert consumer.poll() is None
