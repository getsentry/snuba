import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from typing import Iterator, Optional
from unittest import mock

import pytest
from arroyo import Message, Partition, Topic
from arroyo.backends.kafka import KafkaPayload, KafkaProducer
from arroyo.backends.local.backend import LocalBroker as Broker
from arroyo.backends.local.storages.memory import MemoryMessageStorage
from arroyo.processing.strategies import MessageRejected
from arroyo.utils.clock import TestingClock
from confluent_kafka.admin import AdminClient

from snuba import state
from snuba.datasets.entities import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.factory import get_dataset
from snuba.reader import Result
from snuba.subscriptions.codecs import SubscriptionScheduledTaskEncoder
from snuba.subscriptions.data import (
    PartitionId,
    ScheduledSubscriptionTask,
    SnQLSubscriptionData,
    Subscription,
    SubscriptionIdentifier,
    SubscriptionTaskResult,
    SubscriptionWithMetadata,
)
from snuba.subscriptions.entity_subscription import EventsSubscription
from snuba.subscriptions.executor_consumer import (
    ExecuteQuery,
    ProduceResult,
    build_executor_consumer,
)
from snuba.utils.manage_topics import create_topics
from snuba.utils.metrics.timer import Timer
from snuba.utils.streams.configuration_builder import (
    build_kafka_producer_configuration,
    get_default_kafka_configuration,
)
from snuba.utils.streams.topics import Topic as SnubaTopic
from tests.backends.metrics import TestingMetricsBackend


@pytest.mark.ci_only
def test_executor_consumer() -> None:
    state.set_config("executor_sample_rate", 1.0)
    admin_client = AdminClient(get_default_kafka_configuration())
    create_topics(admin_client, [SnubaTopic.SUBSCRIPTION_SCHEDULED_EVENTS])

    dataset_name = "events"
    entity_name = "events"
    entity_key = EntityKey(entity_name)
    entity = get_entity(entity_key)
    storage = entity.get_writable_storage()
    assert storage is not None
    stream_loader = storage.get_table_writer().get_stream_loader()

    scheduled_topic_spec = stream_loader.get_subscription_scheduled_topic_spec()
    assert scheduled_topic_spec is not None

    scheduled_topic = Topic(scheduled_topic_spec.topic.name)

    producer = KafkaProducer(
        build_kafka_producer_configuration(scheduled_topic_spec.topic)
    )

    consumer_group = str(uuid.uuid1().hex)
    auto_offset_reset = "latest"
    executor = build_executor_consumer(
        dataset_name,
        [entity_name],
        consumer_group,
        producer,
        2,
        auto_offset_reset,
        TestingMetricsBackend(),
        override_result_topic="test-result-topic",
    )

    # Produce a scheduled task to the scheduled subscriptions topic
    subscription_data = SnQLSubscriptionData(
        project_id=1,
        query="MATCH events SELECT count()",
        time_window=timedelta(minutes=1),
        resolution=timedelta(minutes=1),
        entity_subscription=EventsSubscription(data_dict={}),
    )

    subscription_id = uuid.UUID("91b46cb6224f11ecb2ddacde48001122")

    epoch = datetime(1970, 1, 1)

    task = ScheduledSubscriptionTask(
        timestamp=epoch,
        task=SubscriptionWithMetadata(
            entity_key,
            Subscription(
                SubscriptionIdentifier(PartitionId(1), subscription_id),
                subscription_data,
            ),
            1,
        ),
    )

    encoder = SubscriptionScheduledTaskEncoder()

    encoded_task = encoder.encode(task)

    producer.produce(scheduled_topic, payload=encoded_task).result()

    producer.close()

    executor._run_once()

    executor._shutdown()


def generate_message(
    subscription_identifier: Optional[SubscriptionIdentifier] = None,
) -> Iterator[Message[KafkaPayload]]:
    codec = SubscriptionScheduledTaskEncoder()
    epoch = datetime(1970, 1, 1)
    i = 0

    if subscription_identifier is None:
        subscription_identifier = SubscriptionIdentifier(PartitionId(1), uuid.uuid1())

    while True:
        payload = codec.encode(
            ScheduledSubscriptionTask(
                epoch + timedelta(minutes=i),
                SubscriptionWithMetadata(
                    EntityKey.EVENTS,
                    Subscription(
                        subscription_identifier,
                        SnQLSubscriptionData(
                            project_id=1,
                            time_window=timedelta(minutes=1),
                            resolution=timedelta(minutes=1),
                            query="MATCH (events) SELECT count()",
                            entity_subscription=EventsSubscription(data_dict={}),
                        ),
                    ),
                    i + 1,
                ),
            )
        )

        yield Message(Partition(Topic("test"), 0), i, payload, epoch, i + 1)
        i += 1


def test_execute_query_strategy() -> None:
    state.set_config("executor_sample_rate", 1.0)
    dataset = get_dataset("events")
    entity_names = ["events"]
    max_concurrent_queries = 2
    executor = ThreadPoolExecutor(max_concurrent_queries)
    metrics = TestingMetricsBackend()
    next_step = mock.Mock()

    strategy = ExecuteQuery(
        dataset, entity_names, executor, max_concurrent_queries, metrics, next_step
    )

    make_message = generate_message()
    message = next(make_message)

    strategy.submit(message)

    # next_step.submit should be called eventually
    while next_step.submit.call_count == 0:
        time.sleep(0.1)
        strategy.poll()

    assert next_step.submit.call_args[0][0].timestamp == message.timestamp
    assert next_step.submit.call_args[0][0].offset == message.offset

    result = next_step.submit.call_args[0][0].payload.result
    assert result[1]["data"] == [{"count()": 0}]
    assert result[1]["meta"] == [{"name": "count()", "type": "UInt64"}]

    strategy.close()
    strategy.join()


def test_too_many_concurrent_queries() -> None:
    state.set_config("executor_sample_rate", 1.0)
    dataset = get_dataset("events")
    entity_names = ["events"]
    executor = ThreadPoolExecutor(2)
    metrics = TestingMetricsBackend()
    next_step = mock.Mock()

    strategy = ExecuteQuery(dataset, entity_names, executor, 4, metrics, next_step)

    make_message = generate_message()

    for _ in range(4):
        strategy.submit(next(make_message))

    with pytest.raises(MessageRejected):
        strategy.submit(next(make_message))

    strategy.close()
    strategy.join()


def test_produce_result() -> None:
    epoch = datetime(1970, 1, 1)
    scheduled_topic = Topic("scheduled-subscriptions-events")
    result_topic = Topic("events-subscriptions-results")
    clock = TestingClock()
    broker_storage: MemoryMessageStorage[KafkaPayload] = MemoryMessageStorage()
    broker: Broker[KafkaPayload] = Broker(broker_storage, clock)
    broker.create_topic(scheduled_topic, partitions=1)
    broker.create_topic(result_topic, partitions=1)

    producer = broker.get_producer()
    commit = mock.Mock()

    strategy = ProduceResult(producer, result_topic.name, commit)

    subscription_data = SnQLSubscriptionData(
        project_id=1,
        query="MATCH (events) SELECT count() AS count",
        time_window=timedelta(minutes=1),
        resolution=timedelta(minutes=1),
        entity_subscription=EventsSubscription(data_dict={}),
    )

    subscription = Subscription(
        SubscriptionIdentifier(PartitionId(0), uuid.uuid1()), subscription_data
    )

    request = subscription_data.build_request(
        get_dataset("events"), epoch, None, Timer("timer")
    )
    result: Result = {
        "meta": [{"type": "UInt64", "name": "count"}],
        "data": [{"count": 1}],
    }

    message = Message(
        Partition(scheduled_topic, 0),
        1,
        SubscriptionTaskResult(
            ScheduledSubscriptionTask(
                epoch, SubscriptionWithMetadata(EntityKey.EVENTS, subscription, 1),
            ),
            (request, result),
        ),
        epoch,
        2,
    )

    strategy.submit(message)

    produced_message = broker_storage.consume(Partition(result_topic, 0), 0)
    assert produced_message is not None
    assert produced_message.payload.key == str(subscription.identifier).encode("utf-8")
    assert broker_storage.consume(Partition(result_topic, 0), 1) is None
    assert commit.call_count == 0
    strategy.poll()
    assert commit.call_count == 1


def test_execute_and_produce_result() -> None:
    state.set_config("executor_sample_rate", 1.0)
    dataset = get_dataset("events")
    entity_names = ["events"]
    executor = ThreadPoolExecutor()
    max_concurrent_queries = 2
    metrics = TestingMetricsBackend()

    scheduled_topic = Topic("scheduled-subscriptions-events")
    result_topic = Topic("events-subscriptions-results")
    clock = TestingClock()
    broker_storage: MemoryMessageStorage[KafkaPayload] = MemoryMessageStorage()
    broker: Broker[KafkaPayload] = Broker(broker_storage, clock)
    broker.create_topic(scheduled_topic, partitions=1)
    broker.create_topic(result_topic, partitions=1)
    producer = broker.get_producer()

    commit = mock.Mock()

    strategy = ExecuteQuery(
        dataset,
        entity_names,
        executor,
        max_concurrent_queries,
        metrics,
        ProduceResult(producer, result_topic.name, commit),
    )

    subscription_identifier = SubscriptionIdentifier(PartitionId(0), uuid.uuid1())

    make_message = generate_message(subscription_identifier)
    message = next(make_message)
    strategy.submit(message)

    # Eventually a message should be produced and offsets committed
    while (
        broker_storage.consume(Partition(result_topic, 0), 0) is None
        or commit.call_count == 0
    ):
        strategy.poll()

    produced_message = broker_storage.consume(Partition(result_topic, 0), 0)
    assert produced_message is not None
    assert produced_message.payload.key == str(subscription_identifier).encode("utf-8")
    assert commit.call_count == 1
