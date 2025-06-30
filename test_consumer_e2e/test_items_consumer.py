import subprocess
import time
import uuid
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from typing import Generator, Optional

from confluent_kafka import Message as KafkaMessage
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from google.protobuf.timestamp_pb2 import Timestamp
from sentry_protos.snuba.v1.request_common_pb2 import TraceItemType
from sentry_protos.snuba.v1.trace_item_pb2 import AnyValue, TraceItem

from snuba.clusters.cluster import ClickhouseClientSettings
from snuba.datasets.storages.factory import get_storage
from snuba.datasets.storages.storage_key import StorageKey

# Kafka configuration
kafka_config = {"bootstrap.servers": "localhost:9092", "client.id": "items_producer"}

producer = Producer(kafka_config)


def generate_item_message(start_timestamp: Optional[datetime] = None) -> bytes:
    if start_timestamp is None:
        start_timestamp = datetime.now(tz=UTC)

    item_timestamp = Timestamp()
    item_timestamp.FromDatetime(start_timestamp)

    received = Timestamp()
    received.GetCurrentTime()

    end_timestamp = start_timestamp + timedelta(seconds=1)

    attributes = {
        "category": AnyValue(string_value="http"),
        "description": AnyValue(string_value="/api/0/events/"),
        "environment": AnyValue(string_value="production"),
        "http.status_code": AnyValue(string_value="200"),
        "op": AnyValue(string_value="http.server"),
        "platform": AnyValue(string_value="python"),
        "sentry.received": AnyValue(double_value=received.seconds),
        "sentry.start_timestamp_precise": AnyValue(
            double_value=start_timestamp.timestamp()
        ),
        "sentry.end_timestamp_precise": AnyValue(
            double_value=end_timestamp.timestamp()
        ),
        "start_timestamp_ms": AnyValue(
            double_value=int(start_timestamp.timestamp() * 1000)
        ),
    }

    return TraceItem(
        organization_id=1,
        project_id=1,
        item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
        timestamp=item_timestamp,
        trace_id=uuid.uuid4().hex,
        item_id=uuid.uuid4().int.to_bytes(16, byteorder="little"),
        received=received,
        retention_days=90,
        server_sample_rate=1.0,
        attributes=attributes,
    ).SerializeToString()


@contextmanager
def tmp_kafka_topic() -> Generator[str, None, None]:
    # Create a unique topic name
    topic_name = f"snuba-items-{uuid.uuid4().hex}"

    # Create the topic
    admin_client = AdminClient({"bootstrap.servers": "localhost:9092"})
    new_topic = NewTopic(topic_name, num_partitions=1, replication_factor=1)
    admin_client.create_topics([new_topic])

    try:
        yield topic_name
    finally:
        # Clean up the topic
        admin_client.delete_topics([topic_name])


def test_items_consumer() -> None:
    # Launch the consumer process
    with tmp_kafka_topic() as topic:
        consumer_process = subprocess.Popen(
            [
                "snuba",
                "rust-consumer",
                "--raw-events-topic",
                topic,
                "--consumer-version",
                "v2",
                "--consumer-group",
                f"eap_items_consumer_{uuid.uuid4().hex}",
                "--storage",
                "eap_items",
                "--no-strict-offset-reset",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        storage = get_storage(StorageKey("eap_items"))
        storage.get_cluster().get_query_connection(
            ClickhouseClientSettings.QUERY
        ).execute("TRUNCATE TABLE IF EXISTS eap_items_1_local")

        # Wait for consumer to initialize
        time.sleep(2)
        num_items = 1000

        def delivery_report(err: Optional[Exception], msg: KafkaMessage) -> None:
            if err is not None:
                raise err

        try:
            count = 0
            while count < num_items:
                count += 1
                if count % 100 == 0:
                    producer.poll(0)
                message = generate_item_message()
                producer.produce(topic, message, callback=delivery_report)
        finally:
            # Wait for any outstanding messages to be delivered
            producer.flush()

        res = (
            storage.get_cluster()
            .get_query_connection(ClickhouseClientSettings.QUERY)
            .execute("SELECT count(*) FROM default.eap_items_1_local")
        )
        try:
            consumer_process.terminate()
        except Exception:
            pass
        assert res.results[0][0] == num_items
