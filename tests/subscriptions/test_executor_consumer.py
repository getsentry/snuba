import uuid
from datetime import datetime, timedelta

from arroyo import Topic
from arroyo.backends.kafka import KafkaProducer

from snuba.datasets.entities import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.subscriptions.codecs import SubscriptionScheduledTaskEncoder
from snuba.subscriptions.data import (
    PartitionId,
    ScheduledSubscriptionTask,
    SnQLSubscriptionData,
    Subscription,
    SubscriptionIdentifier,
    SubscriptionWithTick,
)
from snuba.subscriptions.entity_subscription import EventsSubscription
from snuba.subscriptions.executor_consumer import ExecutorBuilder
from snuba.subscriptions.utils import Tick
from snuba.utils.streams.configuration_builder import build_kafka_producer_configuration
from snuba.utils.types import Interval
from tests.backends.metrics import TestingMetricsBackend


def test_executor_consumer() -> None:
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
    builder = ExecutorBuilder(
        dataset_name,
        entity_name,
        consumer_group,
        2,
        auto_offset_reset,
        TestingMetricsBackend(),
    )

    executor = builder.build_consumer()

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
        task=SubscriptionWithTick(
            Subscription(
                SubscriptionIdentifier(PartitionId(1), subscription_id),
                subscription_data,
            ),
            Tick(
                0, Interval(0, 1), Interval(datetime(1970, 1, 1), datetime(1970, 1, 2)),
            ),
        ),
    )

    encoder = SubscriptionScheduledTaskEncoder(entity_key)

    encoded_task = encoder.encode(task)

    fut = producer.produce(topic, payload=encoded_task)
    fut.result()

    producer.close()

    executor._run_once()

    executor._shutdown()
