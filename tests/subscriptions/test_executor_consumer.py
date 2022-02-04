import json
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from typing import Iterator, Mapping, Optional
from unittest import mock

import pytest
from arroyo import Message, Partition, Topic
from arroyo.backends.kafka import KafkaConsumer, KafkaPayload, KafkaProducer
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
from snuba.subscriptions.entity_subscription import (
    ENTITY_KEY_TO_SUBSCRIPTION_MAPPER,
    EventsSubscription,
)
from snuba.subscriptions.executor_consumer import (
    ExecuteQuery,
    ProduceResult,
    build_executor_consumer,
)
from snuba.utils.manage_topics import create_topics
from snuba.utils.metrics.timer import Timer
from snuba.utils.streams.configuration_builder import (
    build_kafka_consumer_configuration,
    build_kafka_producer_configuration,
    get_default_kafka_configuration,
)
from snuba.utils.streams.topics import Topic as SnubaTopic
from tests.backends.metrics import Increment, TestingMetricsBackend


@pytest.mark.ci_only
def test_executor_consumer() -> None:
    """
    End to end integration test
    """

    state.set_config("executor_sample_rate", 1.0)
    admin_client = AdminClient(get_default_kafka_configuration())
    create_topics(admin_client, [SnubaTopic.SUBSCRIPTION_SCHEDULED_EVENTS])
    create_topics(admin_client, [SnubaTopic.SUBSCRIPTION_RESULTS_EVENTS])

    dataset_name = "events"
    entity_name = "events"
    entity_key = EntityKey(entity_name)
    entity = get_entity(entity_key)
    storage = entity.get_writable_storage()
    assert storage is not None
    stream_loader = storage.get_table_writer().get_stream_loader()

    scheduled_result_topic_spec = stream_loader.get_subscription_result_topic_spec()
    assert scheduled_result_topic_spec is not None
    result_producer = KafkaProducer(
        build_kafka_producer_configuration(scheduled_result_topic_spec.topic)
    )

    result_consumer = KafkaConsumer(
        build_kafka_consumer_configuration(
            scheduled_result_topic_spec.topic,
            str(uuid.uuid1().hex),
            auto_offset_reset="latest",
        )
    )
    assigned = False

    def on_partitions_assigned(partitions: Mapping[Partition, int]) -> None:
        nonlocal assigned
        assigned = True

    result_consumer.subscribe(
        [Topic(scheduled_result_topic_spec.topic_name)],
        on_assign=on_partitions_assigned,
    )

    attempts = 10
    while attempts > 0 and not assigned:
        result_consumer.poll(1.0)
        attempts -= 1

    # We need to wait for the consumer to receive partitions otherwise,
    # when we try to consume messages, we will not find anything.
    # Subscription is an async process.
    assert assigned == True, "Did not receive assignment within 10 attempts"

    consumer_group = str(uuid.uuid1().hex)
    auto_offset_reset = "latest"
    executor = build_executor_consumer(
        dataset_name,
        [entity_name],
        consumer_group,
        result_producer,
        2,
        auto_offset_reset,
        TestingMetricsBackend(),
        ThreadPoolExecutor(2),
        override_result_topic=scheduled_result_topic_spec.topic.value,
    )
    for i in range(1, 5):
        # Give time to the executor to subscribe
        time.sleep(1)
        executor._run_once()

    # Produce a scheduled task to the scheduled subscriptions topic
    subscription_data = SnQLSubscriptionData(
        project_id=1,
        query="MATCH (events) SELECT count()",
        time_window=timedelta(minutes=1),
        resolution=timedelta(minutes=1),
        entity_subscription=EventsSubscription(data_dict={}),
    )

    task = ScheduledSubscriptionTask(
        timestamp=datetime(1970, 1, 1),
        task=SubscriptionWithMetadata(
            entity_key,
            Subscription(
                SubscriptionIdentifier(
                    PartitionId(1), uuid.UUID("91b46cb6224f11ecb2ddacde48001122")
                ),
                subscription_data,
            ),
            1,
        ),
    )

    encoder = SubscriptionScheduledTaskEncoder()
    encoded_task = encoder.encode(task)

    scheduled_topic_spec = stream_loader.get_subscription_scheduled_topic_spec()
    assert scheduled_topic_spec is not None
    tasks_producer = KafkaProducer(
        build_kafka_producer_configuration(scheduled_topic_spec.topic)
    )

    scheduled_topic = Topic(scheduled_topic_spec.topic_name)
    tasks_producer.produce(scheduled_topic, payload=encoded_task).result()
    tasks_producer.close()

    executor._run_once()
    executor.signal_shutdown()
    # Call run here so that the executor shuts down itself cleanly.
    executor.run()
    result = result_consumer.poll(5)
    assert result is not None, "Did not receive a result message"
    data = json.loads(result.payload.value)
    assert (
        data["payload"]["subscription_id"] == "1/91b46cb6224f11ecb2ddacde48001122"
    ), "Invalid subscription id"

    result_producer.close()


def generate_message(
    entity_key: EntityKey,
    subscription_identifier: Optional[SubscriptionIdentifier] = None,
) -> Iterator[Message[KafkaPayload]]:
    codec = SubscriptionScheduledTaskEncoder()
    epoch = datetime(1970, 1, 1)
    i = 0

    if subscription_identifier is None:
        subscription_identifier = SubscriptionIdentifier(PartitionId(1), uuid.uuid1())

    data_dict = {}
    if entity_key in (EntityKey.METRICS_SETS, EntityKey.METRICS_COUNTERS):
        data_dict = {"organization": 1}

    entity_subscription = ENTITY_KEY_TO_SUBSCRIPTION_MAPPER[entity_key](
        data_dict=data_dict
    )

    while True:
        payload = codec.encode(
            ScheduledSubscriptionTask(
                epoch + timedelta(minutes=i),
                SubscriptionWithMetadata(
                    entity_key,
                    Subscription(
                        subscription_identifier,
                        SnQLSubscriptionData(
                            project_id=1,
                            time_window=timedelta(minutes=1),
                            resolution=timedelta(minutes=1),
                            query=f"MATCH ({entity_key.value}) SELECT count()",
                            entity_subscription=entity_subscription,
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

    make_message = generate_message(EntityKey.EVENTS)
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
    state.set_config("executor_queue_size_factor", 1)
    dataset = get_dataset("events")
    entity_names = ["events"]
    executor = ThreadPoolExecutor(2)
    metrics = TestingMetricsBackend()
    next_step = mock.Mock()

    strategy = ExecuteQuery(dataset, entity_names, executor, 4, metrics, next_step)

    make_message = generate_message(EntityKey.EVENTS)

    for _ in range(4):
        strategy.submit(next(make_message))

    with pytest.raises(MessageRejected):
        strategy.submit(next(make_message))

    strategy.close()
    strategy.join()


def test_skip_execution_for_entity() -> None:
    # Skips execution if the entity name is not on the list
    state.set_config("executor_sample_rate", 1.0)
    dataset = get_dataset("metrics")
    entity_names = ["metrics_sets"]
    executor = ThreadPoolExecutor()
    metrics = TestingMetricsBackend()
    next_step = mock.Mock()

    strategy = ExecuteQuery(dataset, entity_names, executor, 4, metrics, next_step)

    metrics_sets_message = next(generate_message(EntityKey.METRICS_SETS))
    strategy.submit(metrics_sets_message)
    metrics_counters_message = next(generate_message(EntityKey.METRICS_COUNTERS))
    strategy.submit(metrics_counters_message)

    assert (
        Increment("skipped_execution", 1, {"entity": "metrics_sets"})
        not in metrics.calls
    )
    assert (
        Increment("skipped_execution", 1, {"entity": "metrics_counters"})
        in metrics.calls
    )


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

    make_message = generate_message(EntityKey.EVENTS, subscription_identifier)
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
