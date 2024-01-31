import json
import time
import uuid
from datetime import datetime, timedelta
from typing import Iterator, Mapping, Optional
from unittest import mock

import pytest
from arroyo.backends.kafka import KafkaConsumer, KafkaPayload, KafkaProducer
from arroyo.backends.local.backend import LocalBroker as Broker
from arroyo.backends.local.storages.memory import MemoryMessageStorage
from arroyo.processing.strategies import MessageRejected
from arroyo.processing.strategies.commit import CommitOffsets
from arroyo.processing.strategies.produce import Produce
from arroyo.types import BrokerValue, Message, Partition, Topic
from arroyo.utils.clock import TestingClock
from confluent_kafka.admin import AdminClient

from snuba import state
from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.factory import get_dataset
from snuba.subscriptions.codecs import SubscriptionScheduledTaskEncoder
from snuba.subscriptions.data import (
    PartitionId,
    ScheduledSubscriptionTask,
    Subscription,
    SubscriptionData,
    SubscriptionIdentifier,
    SubscriptionWithMetadata,
)
from snuba.subscriptions.executor_consumer import (
    ExecuteQuery,
    build_executor_consumer,
    calculate_max_concurrent_queries,
)
from snuba.utils.manage_topics import create_topics
from snuba.utils.streams.configuration_builder import (
    build_kafka_consumer_configuration,
    build_kafka_producer_configuration,
    get_default_kafka_configuration,
)
from snuba.utils.streams.topics import Topic as SnubaTopic
from tests.backends.metrics import Increment, TestingMetricsBackend


@pytest.mark.ci_only
@pytest.mark.clickhouse_db
@pytest.mark.redis_db
def test_executor_consumer() -> None:
    """
    End to end integration test
    """
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
            strict_offset_reset=False,
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
    strict_offset_reset = False
    executor = build_executor_consumer(
        dataset_name,
        [entity_name],
        consumer_group,
        [],
        None,
        result_producer,
        2,
        auto_offset_reset,
        strict_offset_reset,
        TestingMetricsBackend(),
        None,
    )
    for i in range(1, 5):
        # Give time to the executor to subscribe
        time.sleep(1)
        executor._run_once()

    # Produce a scheduled task to the scheduled subscriptions topic
    entity = get_entity(EntityKey.EVENTS)
    subscription_data = SubscriptionData(
        project_id=1,
        query="MATCH (events) SELECT count()",
        time_window_sec=60,
        resolution_sec=60,
        entity=entity,
        metadata={},
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

    metadata = {}
    if entity_key in (EntityKey.METRICS_SETS, EntityKey.METRICS_COUNTERS):
        metadata.update({"organization": 1})

    entity = get_entity(entity_key)

    while True:
        payload = codec.encode(
            ScheduledSubscriptionTask(
                epoch + timedelta(minutes=i),
                SubscriptionWithMetadata(
                    entity_key,
                    Subscription(
                        subscription_identifier,
                        SubscriptionData(
                            project_id=1,
                            time_window_sec=60,
                            resolution_sec=60,
                            query=f"MATCH ({entity_key.value}) SELECT count()",
                            entity=entity,
                            metadata=metadata,
                        ),
                    ),
                    i + 1,
                ),
            )
        )

        yield Message(BrokerValue(payload, Partition(Topic("test"), 0), i, epoch))
        i += 1


@pytest.mark.redis_db
@pytest.mark.clickhouse_db
def test_execute_query_strategy() -> None:
    next_step = mock.Mock()

    strategy = ExecuteQuery(
        dataset=get_dataset("events"),
        entity_names=["events"],
        max_concurrent_queries=2,
        stale_threshold_seconds=None,
        metrics=TestingMetricsBackend(),
        next_step=next_step,
    )

    make_message = generate_message(EntityKey.EVENTS)
    message = next(make_message)

    strategy.submit(message)

    # next_step.submit should be called eventually
    while next_step.submit.call_count == 0:
        time.sleep(0.1)
        strategy.poll()

    assert isinstance(message.value, BrokerValue)
    assert next_step.submit.call_args[0][0].committable == message.committable

    result = json.loads(next_step.submit.call_args[0][0].payload.value)["payload"][
        "result"
    ]

    assert result["data"] == [{"count()": 0}]
    assert result["meta"] == [{"name": "count()", "type": "UInt64"}]

    strategy.close()
    strategy.join()


@pytest.mark.redis_db
@pytest.mark.clickhouse_db
def test_too_many_concurrent_queries() -> None:
    state.set_config("executor_queue_size_factor", 1)

    strategy = ExecuteQuery(
        dataset=get_dataset("events"),
        entity_names=["events"],
        max_concurrent_queries=4,
        stale_threshold_seconds=None,
        metrics=TestingMetricsBackend(),
        next_step=mock.Mock(),
    )

    make_message = generate_message(EntityKey.EVENTS)

    for _ in range(4):
        strategy.submit(next(make_message))

    with pytest.raises(MessageRejected):
        strategy.submit(next(make_message))

    strategy.close()
    strategy.join()


@pytest.mark.clickhouse_db
@pytest.mark.redis_db
def test_skip_execution_for_entity() -> None:
    metrics = TestingMetricsBackend()

    strategy = ExecuteQuery(
        # Skips execution if the entity name is not on the list
        dataset=get_dataset("metrics"),
        entity_names=["metrics_sets"],
        max_concurrent_queries=4,
        stale_threshold_seconds=None,
        metrics=metrics,
        next_step=mock.Mock(),
    )

    metrics_sets_message = next(generate_message(EntityKey.METRICS_SETS))
    strategy.submit(metrics_sets_message)
    metrics_counters_message = next(generate_message(EntityKey.METRICS_COUNTERS))
    strategy.submit(metrics_counters_message)

    strategy.close()
    strategy.join()

    assert (
        Increment("skipped_execution", 1, {"entity": "metrics_sets"})
        not in metrics.calls
    )
    assert (
        Increment("skipped_execution", 1, {"entity": "metrics_counters"})
        in metrics.calls
    )


@pytest.mark.clickhouse_db
@pytest.mark.redis_db
def test_execute_and_produce_result() -> None:
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
        dataset=get_dataset("events"),
        entity_names=["events"],
        max_concurrent_queries=2,
        stale_threshold_seconds=None,
        metrics=TestingMetricsBackend(),
        next_step=Produce(producer, Topic(result_topic.name), CommitOffsets(commit)),
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
    assert commit.call_count > 0


def test_skip_stale_message() -> None:
    scheduled_topic = Topic("scheduled-subscriptions-events")
    result_topic = Topic("events-subscriptions-results")
    clock = TestingClock()
    broker_storage: MemoryMessageStorage[KafkaPayload] = MemoryMessageStorage()
    broker: Broker[KafkaPayload] = Broker(broker_storage, clock)
    broker.create_topic(scheduled_topic, partitions=1)
    broker.create_topic(result_topic, partitions=1)
    producer = broker.get_producer()

    metrics = TestingMetricsBackend()
    strategy = ExecuteQuery(
        dataset=get_dataset("events"),
        entity_names=["events"],
        max_concurrent_queries=2,
        stale_threshold_seconds=60,
        metrics=metrics,
        next_step=Produce(producer, Topic(result_topic.name), mock.Mock()),
    )

    subscription_identifier = SubscriptionIdentifier(PartitionId(0), uuid.uuid1())

    make_message = generate_message(EntityKey.EVENTS, subscription_identifier)
    message = next(make_message)
    strategy.submit(message)

    # No message will be produced
    strategy.poll()
    assert broker_storage.consume(Partition(result_topic, 0), 0) is None
    assert Increment("skipped_execution", 1, {"entity": "events"}) in metrics.calls


# Formula for the max_concurrent_queries found in executor_consumer.py:
# math.ceil(total_concurrent_queries / (math.ceil(partition_count / len(partitions)) or 1)
max_concurrent_queries_tests = [
    # 1. 6 / (12/4) == 6/3 ==> 2 max concurrent queries
    pytest.param(
        4,
        12,
        6,
        2,
        id="len(partitions) == 4",
    ),
    # 2. 6 / (12/2) == 6/6 ==> 1 max concurrent queries
    pytest.param(
        2,
        12,
        6,
        1,
        id="len(partitions) == 2",
    ),
    # 3. 6 / (12/1) == 6/12 ==> 1 max concurrent queries
    pytest.param(
        1,
        12,
        6,
        1,
        id="len(partitions) == 1",
    ),
]


@pytest.mark.parametrize(
    "assigned_partition_count, total_partition_count, total_concurrent_queries, expected_max_concurrent_queries",
    max_concurrent_queries_tests,
)
def test_max_concurrent_queries(
    assigned_partition_count: int,
    total_partition_count: int,
    total_concurrent_queries: int,
    expected_max_concurrent_queries: int,
) -> None:

    calculated = calculate_max_concurrent_queries(
        assigned_partition_count, total_partition_count, total_concurrent_queries
    )
    assert calculated == expected_max_concurrent_queries
