import time
import uuid
from datetime import datetime, timedelta

from arroyo import Partition, Topic
from arroyo.backends.kafka import KafkaProducer
from arroyo.synchronized import Commit, commit_codec
from confluent_kafka.admin import AdminClient

from snuba.datasets.entities import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.subscriptions.scheduler_consumer import SchedulerBuilder
from snuba.utils.manage_topics import create_topics
from snuba.utils.streams.configuration_builder import (
    build_kafka_producer_configuration,
    get_default_kafka_configuration,
)
from snuba.utils.streams.topics import Topic as SnubaTopic
from tests.backends.metrics import TestingMetricsBackend, Timing


def test_scheduler_consumer() -> None:
    admin_client = AdminClient(get_default_kafka_configuration())
    create_topics(admin_client, [SnubaTopic.COMMIT_LOG])

    metrics_backend = TestingMetricsBackend()
    entity_name = "events"
    entity = get_entity(EntityKey(entity_name))
    storage = entity.get_writable_storage()
    assert storage is not None
    stream_loader = storage.get_table_writer().get_stream_loader()

    builder = SchedulerBuilder(
        entity_name, 2, str(uuid.uuid1().hex), "latest", None, metrics_backend
    )
    scheduler = builder.build_consumer()
    time.sleep(2)
    scheduler._run_once()
    scheduler._run_once()
    scheduler._run_once()

    topic = Topic("snuba-commit-log")

    now = datetime.now() - timedelta(seconds=1)

    producer = KafkaProducer(
        build_kafka_producer_configuration(
            stream_loader.get_default_topic_spec().topic,
        )
    )

    for (partition, offset, orig_message_ts) in [
        (0, 0, now - timedelta(seconds=6)),
        (1, 0, now - timedelta(seconds=4)),
        (0, 1, now - timedelta(seconds=2)),
        (1, 1, now),
    ]:
        fut = producer.produce(
            topic,
            payload=commit_codec.encode(
                Commit("events", Partition(topic, partition), offset, orig_message_ts)
            ),
        )
        fut.result()

    producer.close()

    while len(metrics_backend.calls) == 0:
        scheduler._run_once()

    scheduler._shutdown()

    assert metrics_backend.calls[0] == Timing("partition_lag_ms", 2000, None)
