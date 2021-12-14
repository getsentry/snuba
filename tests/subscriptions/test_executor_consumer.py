import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from typing import Iterator
from unittest import mock

import pytest
from arroyo import Message, Partition, Topic
from arroyo.backends.kafka import KafkaPayload, KafkaProducer
from arroyo.processing.strategies import MessageRejected
from confluent_kafka.admin import AdminClient

from snuba.datasets.entities import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.factory import get_dataset
from snuba.subscriptions.codecs import SubscriptionScheduledTaskEncoder
from snuba.subscriptions.data import (
    PartitionId,
    ScheduledSubscriptionTask,
    SnQLSubscriptionData,
    Subscription,
    SubscriptionIdentifier,
    SubscriptionWithMetadata,
)
from snuba.subscriptions.entity_subscription import EventsSubscription
from snuba.subscriptions.executor_consumer import ExecuteQuery, build_executor_consumer
from snuba.utils.manage_topics import create_topics
from snuba.utils.streams.configuration_builder import (
    build_kafka_producer_configuration,
    get_default_kafka_configuration,
)
from snuba.utils.streams.topics import Topic as SnubaTopic
from tests.backends.metrics import TestingMetricsBackend


@pytest.mark.ci_only
def test_executor_consumer() -> None:
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

    topic = Topic(scheduled_topic_spec.topic.name)

    consumer_group = str(uuid.uuid1().hex)
    auto_offset_reset = "latest"
    executor = build_executor_consumer(
        dataset_name,
        [entity_name],
        consumer_group,
        2,
        auto_offset_reset,
        TestingMetricsBackend(),
    )

    # Produce a scheduled task to the scheduled subscriptions topic
    producer = KafkaProducer(
        build_kafka_producer_configuration(scheduled_topic_spec.topic)
    )

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

    fut = producer.produce(topic, payload=encoded_task)
    fut.result()

    producer.close()

    executor._run_once()

    executor._shutdown()


def generate_message() -> Iterator[Message[KafkaPayload]]:
    codec = SubscriptionScheduledTaskEncoder()
    epoch = datetime(1970, 1, 1)
    i = 0

    while True:
        payload = codec.encode(
            ScheduledSubscriptionTask(
                epoch + timedelta(minutes=i),
                SubscriptionWithMetadata(
                    EntityKey.EVENTS,
                    Subscription(
                        SubscriptionIdentifier(PartitionId(1), uuid.uuid1()),
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
    dataset = get_dataset("events")
    max_concurrent_queries = 2
    executor = ThreadPoolExecutor(max_concurrent_queries)
    metrics = TestingMetricsBackend()
    next_step = mock.Mock()

    strategy = ExecuteQuery(
        dataset, executor, max_concurrent_queries, metrics, next_step
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
    result = next_step.submit.call_args[0][0].payload.result()
    assert result[1]["data"] == [{"count()": 0}]
    assert result[1]["meta"] == [{"name": "count()", "type": "UInt64"}]

    strategy.close()
    strategy.join()


def test_too_many_concurrent_queries() -> None:
    dataset = get_dataset("events")
    executor = ThreadPoolExecutor(2)
    metrics = TestingMetricsBackend()
    next_step = mock.Mock()

    strategy = ExecuteQuery(dataset, executor, 4, metrics, next_step)

    make_message = generate_message()

    for _ in range(4):
        strategy.submit(next(make_message))

    with pytest.raises(MessageRejected):
        strategy.submit(next(make_message))

    strategy.close()
    strategy.join()
