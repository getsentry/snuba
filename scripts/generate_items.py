import time
import uuid
from datetime import UTC, datetime, timedelta
from typing import Optional

from confluent_kafka import Message as KafkaMessage
from confluent_kafka import Producer
from google.protobuf.timestamp_pb2 import Timestamp
from sentry_protos.snuba.v1.request_common_pb2 import TraceItemType
from sentry_protos.snuba.v1.trace_item_pb2 import AnyValue, TraceItem

# Kafka configuration
kafka_config = {"bootstrap.servers": "localhost:9092", "client.id": "items_producer"}

producer = Producer(kafka_config)


def delivery_report(err: Optional[Exception], msg: KafkaMessage) -> None:
    if err is not None:
        print(f"Message delivery failed: {err}")


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
        "sentry.start_timestamp_precise": AnyValue(double_value=start_timestamp.timestamp()),
        "sentry.end_timestamp_precise": AnyValue(double_value=end_timestamp.timestamp()),
        "start_timestamp_ms": AnyValue(double_value=int(start_timestamp.timestamp() * 1000)),
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


def main() -> None:
    topic = "snuba-items"

    try:
        count = 0
        while True:
            count += 1
            if count % 100 == 0:
                print(f"Generating message {count}...")
            message = generate_item_message()
            producer.produce(topic, message, callback=delivery_report)
            producer.poll(0)
            time.sleep(0.05)  # Produce a message every second

    except KeyboardInterrupt:
        print("Shutting down producer...")
    finally:
        # Wait for any outstanding messages to be delivered
        producer.flush()


if __name__ == "__main__":
    main()
