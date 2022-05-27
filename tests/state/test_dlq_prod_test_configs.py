import json
from datetime import datetime

import pytest
from arroyo import Message, Partition, Topic
from arroyo.backends.kafka import KafkaPayload
from arroyo.processing.strategies.dead_letter_queue import InvalidMessages

from snuba import state
from snuba.consumers.consumer import process_message, skip_kafka_message
from snuba.datasets.metrics_bucket_processor import PolymorphicMetricsProcessor
from snuba.utils.streams.topics import Topic as StreamsTopic


def test_skip_kafka_message() -> None:
    state.set_config(
        "kafka_messages_to_skip", "[snuba-test-lol:1:2,snuba-test-yeet:0:1]"
    )
    assert skip_kafka_message(
        Message(
            Partition(Topic("snuba-test-lol"), 1),
            2,
            KafkaPayload(None, b"", []),
            datetime.now(),
        )
    )
    assert skip_kafka_message(
        Message(
            Partition(Topic("snuba-test-yeet"), 0),
            1,
            KafkaPayload(None, b"", []),
            datetime.now(),
        )
    )
    assert not skip_kafka_message(
        Message(
            Partition(Topic("snuba-test-lol"), 2),
            1,
            KafkaPayload(None, b"", []),
            datetime.now(),
        )
    )


def test_enable_metrics_dlq() -> None:
    invalid_metrics_payload = json.dumps(
        {
            "org_id": 1,
            "project_id": 2,
            "name": "sentry.transactions.transaction.duration",
            "unit": "ms",
            "type": "d",
            "value": ["24.0", 119.0, 146.0, 182.0],
            "timestamp": int(datetime.now().timestamp()),
            "tags": {"6": 91, "9": 134, "4": 117, "5": 7},
            "metric_id": 8,
            "retention_days": 90,
        }
    ).encode("utf-8")

    invalid_metrics_message = Message(
        Partition(Topic(StreamsTopic.METRICS.value), 0),
        0,
        KafkaPayload(None, invalid_metrics_payload, []),
        datetime.now(),
        None,
    )

    # should not raise DLQ exception
    state.set_config("enable_metrics_dlq", 0)
    process_message(
        PolymorphicMetricsProcessor(), "consumer_group", invalid_metrics_message
    )

    # should raise DLQ exception
    state.set_config("enable_metrics_dlq", 1)
    with pytest.raises(InvalidMessages):
        process_message(
            PolymorphicMetricsProcessor(), "consumer_group", invalid_metrics_message
        )
