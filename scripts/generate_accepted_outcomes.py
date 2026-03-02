#!/usr/bin/env python3
"""
Script to generate test messages for the accepted outcomes consumer.

This script produces TraceItem protobuf messages to the snuba-items topic
that can be consumed by the accepted outcomes consumer for local testing.

Usage:
    python scripts/generate_accepted_outcomes.py
    python scripts/generate_accepted_outcomes.py --item-type ERROR
    python scripts/generate_accepted_outcomes.py --count 50 --rate 10
"""

import argparse
import random
import uuid
from collections import Counter
from datetime import UTC, datetime, timedelta
from typing import Optional

from confluent_kafka import Message as KafkaMessage
from confluent_kafka import Producer
from google.protobuf.timestamp_pb2 import Timestamp
from sentry_protos.snuba.v1.request_common_pb2 import TraceItemType
from sentry_protos.snuba.v1.trace_item_pb2 import AnyValue, CategoryCount, Outcomes, TraceItem

# Relay DataCategory values (relay-base-schema/src/data_category.rs)
DATA_CATEGORY_ERROR = 1
DATA_CATEGORY_ATTACHMENT = 4
DATA_CATEGORY_REPLAY = 7
DATA_CATEGORY_SPAN = 12
DATA_CATEGORY_SPAN_INDEXED = 16
DATA_CATEGORY_LOG_ITEM = 23
DATA_CATEGORY_LOG_BYTE = 24


def generate_outcomes(
    item_type: TraceItemType.ValueType,
    key_id: int = 123456,
) -> Outcomes:
    """
    Generate an Outcomes message with appropriate data categories for the given item type.

    Logs have two category counts (LogItem for count, LogByte for size).
    Other types have a single category count.
    """
    if item_type == TraceItemType.TRACE_ITEM_TYPE_SPAN:
        category_counts = [
            CategoryCount(data_category=DATA_CATEGORY_SPAN_INDEXED, quantity=1),
        ]
    elif item_type == TraceItemType.TRACE_ITEM_TYPE_ERROR:
        category_counts = [
            CategoryCount(data_category=DATA_CATEGORY_ERROR, quantity=1),
        ]
    elif item_type == TraceItemType.TRACE_ITEM_TYPE_LOG:
        # Logs produce outcomes in both log_item (count) and log_byte (size) categories
        category_counts = [
            CategoryCount(data_category=DATA_CATEGORY_LOG_ITEM, quantity=1),
            CategoryCount(data_category=DATA_CATEGORY_LOG_BYTE, quantity=512),
        ]
    elif item_type == TraceItemType.TRACE_ITEM_TYPE_REPLAY:
        category_counts = [
            CategoryCount(data_category=DATA_CATEGORY_REPLAY, quantity=1),
        ]
    elif item_type == TraceItemType.TRACE_ITEM_TYPE_ATTACHMENT:
        category_counts = [
            CategoryCount(data_category=DATA_CATEGORY_ATTACHMENT, quantity=1024),
        ]
    else:
        # Default for OCCURRENCE and other types
        category_counts = [
            CategoryCount(data_category=DATA_CATEGORY_ERROR, quantity=1),
        ]

    return Outcomes(category_count=category_counts, key_id=key_id)


def delivery_report(err: Optional[Exception], msg: KafkaMessage) -> None:
    """Callback for Kafka message delivery reports."""
    if err is not None:
        print(f"❌ Message delivery failed: {err}")
    else:
        print(
            f"✓ Message delivered to {msg.topic()} "
            f"[partition {msg.partition()}] at offset {msg.offset()}"
        )


def generate_trace_item_message(
    org_id: int = 1,
    project_id: int = 1,
    item_type: TraceItemType.ValueType = TraceItemType.TRACE_ITEM_TYPE_SPAN,
    retention_days: int = 90,
    start_timestamp: Optional[datetime] = None,
    custom_attributes: Optional[dict] = None,
    key_id: int = 123456,
) -> bytes:
    """
    Generate a single TraceItem protobuf message.

    Args:
        org_id: Organization ID
        project_id: Project ID
        item_type: Type of trace item (SPAN, ERROR, LOG, etc.)
        retention_days: Number of days to retain the data
        start_timestamp: Optional start timestamp for the item
        custom_attributes: Optional custom attributes to add to the item

    Returns:
        Serialized TraceItem protobuf bytes
    """
    if start_timestamp is None:
        start_timestamp = datetime.now(tz=UTC)

    item_timestamp = Timestamp()
    item_timestamp.FromDatetime(start_timestamp)

    received = Timestamp()
    received.GetCurrentTime()

    end_timestamp = start_timestamp + timedelta(seconds=1)

    # Base attributes that work for most item types
    attributes = {
        "environment": AnyValue(string_value="production"),
        "platform": AnyValue(string_value="python"),
        "sentry.received": AnyValue(double_value=received.seconds),
        "sentry.start_timestamp_precise": AnyValue(double_value=start_timestamp.timestamp()),
        "sentry.end_timestamp_precise": AnyValue(double_value=end_timestamp.timestamp()),
        "start_timestamp_ms": AnyValue(double_value=int(start_timestamp.timestamp() * 1000)),
    }

    # Add item-type-specific attributes
    if item_type == TraceItemType.TRACE_ITEM_TYPE_SPAN:
        attributes.update(
            {
                "category": AnyValue(string_value="http"),
                "description": AnyValue(string_value="/api/0/events/"),
                "http.status_code": AnyValue(string_value="200"),
                "op": AnyValue(string_value="http.server"),
            }
        )
    elif item_type == TraceItemType.TRACE_ITEM_TYPE_ERROR:
        attributes.update(
            {
                "error.type": AnyValue(string_value="ValueError"),
                "error.message": AnyValue(string_value="Test error message"),
                "level": AnyValue(string_value="error"),
            }
        )
    elif item_type == TraceItemType.TRACE_ITEM_TYPE_LOG:
        attributes.update(
            {
                "log.level": AnyValue(string_value="info"),
                "log.message": AnyValue(string_value="Test log message"),
            }
        )
    elif item_type == TraceItemType.TRACE_ITEM_TYPE_OCCURRENCE:
        attributes.update(
            {
                "occurrence.type": AnyValue(string_value="test_occurrence"),
                "fingerprint": AnyValue(string_value="test-fingerprint"),
            }
        )

    # Merge in any custom attributes
    if custom_attributes:
        attributes.update(custom_attributes)

    return TraceItem(
        organization_id=org_id,
        project_id=project_id,
        item_type=item_type,
        timestamp=item_timestamp,
        trace_id=uuid.uuid4().hex,
        item_id=uuid.uuid4().int.to_bytes(16, byteorder="little"),
        received=received,
        retention_days=retention_days,
        server_sample_rate=1.0,
        attributes=attributes,
        outcomes=generate_outcomes(item_type, key_id=key_id),
    ).SerializeToString()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate TraceItem messages for the accepted outcomes consumer"
    )
    parser.add_argument(
        "--topic",
        default="snuba-items",
        help="Kafka topic to produce to (default: snuba-items)",
    )
    parser.add_argument(
        "--bootstrap-servers",
        default="localhost:9092",
        help="Kafka bootstrap servers (default: localhost:9092)",
    )
    parser.add_argument(
        "--count",
        type=int,
        default=None,
        help="Number of messages to produce (default: infinite)",
    )
    parser.add_argument(
        "--rate",
        type=float,
        default=1.0,
        help="Messages per second (default: 1.0)",
    )
    parser.add_argument(
        "--org-id",
        type=int,
        nargs="+",
        default=[1],
        help="One or more organization IDs (default: 1)",
    )
    parser.add_argument(
        "--project-id",
        type=int,
        nargs="+",
        default=[1],
        help="One or more project IDs (default: 1)",
    )
    parser.add_argument(
        "--item-type",
        type=str,
        nargs="+",
        default=["SPAN"],
        choices=["SPAN", "ERROR", "LOG", "OCCURRENCE", "REPLAY", "ATTACHMENT"],
        help="One or more TraceItem types (default: SPAN)",
    )
    parser.add_argument(
        "--retention-days",
        type=int,
        default=1,
        help="Retention days (default: 90)",
    )
    parser.add_argument(
        "--key-id",
        type=int,
        default=123456,
        help="DSN key ID for outcomes (default: 123456)",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print each message delivery confirmation",
    )

    args = parser.parse_args()

    # Map item type names to TraceItemType enum values
    item_type_map: dict[str, TraceItemType.ValueType] = {
        "SPAN": TraceItemType.TRACE_ITEM_TYPE_SPAN,
        "ERROR": TraceItemType.TRACE_ITEM_TYPE_ERROR,
        "LOG": TraceItemType.TRACE_ITEM_TYPE_LOG,
        "OCCURRENCE": TraceItemType.TRACE_ITEM_TYPE_OCCURRENCE,
        "REPLAY": TraceItemType.TRACE_ITEM_TYPE_REPLAY,
        "ATTACHMENT": TraceItemType.TRACE_ITEM_TYPE_ATTACHMENT,
    }

    item_type_names = args.item_type  # keep the string names for tracking

    # Initialize Kafka producer
    kafka_config = {
        "bootstrap.servers": args.bootstrap_servers,
        "client.id": "accepted_outcomes_producer",
    }

    producer = Producer(kafka_config)

    # Conditionally set the delivery report callback
    callback = delivery_report if args.verbose else None

    print("🚀 Starting TraceItem producer for accepted outcomes consumer")
    print(f"   Topic: {args.topic}")
    print(f"   Bootstrap servers: {args.bootstrap_servers}")
    print(f"   Org IDs: {args.org_id}, Project IDs: {args.project_id}")
    print(f"   Item Types: {args.item_type}")
    print(f"   Retention: {args.retention_days} days")
    print(f"   Rate: {args.rate} messages/second")
    if args.count:
        print(f"   Count: {args.count} messages")
    else:
        print("   Count: infinite (press Ctrl+C to stop)")
    print()

    def print_counts(counts: Counter) -> None:
        print("📊 Messages per (org_id, project_id, item_type):")
        for (org, proj, itype), n in sorted(counts.items()):
            print(f"   org={org:>6}  project={proj:>6}  type={itype:<12}  count={n}")

    try:
        count = 0
        counts: Counter = Counter()

        while args.count is None or count < args.count:
            count += 1

            org_id = random.choice(args.org_id)
            project_id = random.choice(args.project_id)
            item_type_name = random.choice(item_type_names)
            item_type = item_type_map[item_type_name]

            counts[(org_id, project_id, item_type_name)] += 1

            # Generate TraceItem message
            message_bytes = generate_trace_item_message(
                org_id=org_id,
                project_id=project_id,
                key_id=args.key_id,
                item_type=item_type,
                retention_days=args.retention_days,
            )

            # Produce to Kafka
            producer.produce(
                args.topic,
                value=message_bytes,
                callback=callback,
            )

            # Poll for delivery reports
            producer.poll(0)

            if count % 100 == 0:
                print(f"\n--- {count} messages produced ---")
                print_counts(counts)

    except KeyboardInterrupt:
        print("\n⏹️  Shutting down producer...")
    finally:
        # Wait for any outstanding messages to be delivered
        print("⏳ Flushing remaining messages...")
        producer.flush()
        print(f"\n✅ Produced {count} total messages")
        print_counts(counts)


if __name__ == "__main__":
    main()
