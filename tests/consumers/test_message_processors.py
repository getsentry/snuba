from __future__ import annotations

import json
import time
from datetime import UTC, datetime
from typing import Any, cast
from unittest.mock import patch

import pytest
import sentry_kafka_schemas
from sentry_options.testing import override_options

import rust_snuba
from snuba.consumers.types import KafkaMessageMetadata
from snuba.datasets.processors import DatasetMessageProcessor
from snuba.datasets.processors.eap_items_processor import EAPItemsProcessor
from snuba.datasets.processors.errors_processor import ErrorsProcessor
from snuba.datasets.processors.metrics_bucket_processor import (
    PolymorphicMetricsProcessor,
)
from snuba.datasets.processors.outcomes_processor import OutcomesProcessor
from snuba.datasets.processors.replays_processor import ReplaysProcessor
from snuba.datasets.processors.rust_compat_processor import RustCompatProcessor
from snuba.processor import InsertBatch, ReplacementBatch


@pytest.mark.parametrize(
    "topic,processor",
    [
        ("events", ErrorsProcessor),
        ("outcomes", OutcomesProcessor),
        ("outcomes", OutcomesProcessor),
        ("snuba-metrics", PolymorphicMetricsProcessor),
    ],
)
@patch("snuba.settings.DISCARD_OLD_EVENTS", False)
def test_message_processors(topic: str, processor: type[DatasetMessageProcessor]) -> None:
    """
    Tests the output of Python and Rust message processors is the same
    """
    for ex in sentry_kafka_schemas.iter_examples(topic):
        data_json = ex.load()
        # Hacks to ensure the message isn't rejected with too old
        if topic == "ingest-replay-events":
            data_json["start_time"] = int(time.time())
        if topic == "processed-profiles":
            data_json["received"] = int(time.time())

        processor_name = processor.__qualname__
        partition = 0
        offset = 1
        now = int(time.time())

        rust_processed_message = RustCompatProcessor(processor_name).process_message(
            data_json,
            KafkaMessageMetadata(
                offset=offset,
                partition=partition,
                timestamp=datetime.utcfromtimestamp(now),
            ),
        )

        python_processed_message = processor().process_message(
            data_json,
            KafkaMessageMetadata(
                offset=offset,
                partition=partition,
                timestamp=datetime.utcfromtimestamp(now),
            ),
        )

        if python_processed_message is None:
            assert rust_processed_message is None or (
                isinstance(rust_processed_message, InsertBatch) and not rust_processed_message.rows
            )
        elif isinstance(python_processed_message, ReplacementBatch):
            assert isinstance(rust_processed_message, ReplacementBatch)
            assert rust_processed_message == python_processed_message
        else:
            assert isinstance(rust_processed_message, InsertBatch)
            assert python_processed_message.rows == rust_processed_message.rows


def test_replays_message_processor() -> None:
    """Tests the output of the Replay Python and Rust message processors is the same."""
    processor = ReplaysProcessor
    topic = "ingest-replay-events"

    for ex in sentry_kafka_schemas.iter_examples(topic):
        data_json = ex.load()
        data_json["start_time"] = int(time.time())

        data_bytes = json.dumps(data_json).encode("utf-8")

        processor_name = processor.__qualname__
        partition = 0
        offset = 1
        millis_since_epoch = int(time.time() * 1000)

        rust_processed_message = bytes(
            rust_snuba.process_message(  # type: ignore[attr-defined]
                processor_name, data_bytes, partition, offset, millis_since_epoch
            )[0]
        )
        python_processed_message = processor().process_message(
            data_json,
            KafkaMessageMetadata(
                offset=offset,
                partition=partition,
                timestamp=datetime.utcfromtimestamp(millis_since_epoch / 1000),
            ),
        )

        assert isinstance(python_processed_message, InsertBatch)

        for line in rust_processed_message.rstrip(b"\n").split(b"\n"):
            if not line:
                continue

            parsed_rust_message = json.loads(line)
            parsed_python_message = cast(dict[str, Any], python_processed_message.rows[0])

            # timestamp is sometimes in different formats so we'll coerce.
            ts1 = parsed_rust_message.pop("timestamp", None)
            ts2 = parsed_python_message.pop("timestamp", None)
            if isinstance(ts2, datetime):
                ts2 = int(ts2.timestamp())
            assert ts1 == ts2

            # replay_start_timestamp is sometimes in different formats so we'll coerce.
            sts1 = parsed_rust_message.pop("replay_start_timestamp", None)
            sts2 = parsed_python_message.pop("replay_start_timestamp", None)
            if isinstance(sts2, datetime):
                sts2 = int(sts2.timestamp())
            assert sts1 == sts2

            # event_hash is generated by the consumer and not always consistent if
            # no segment_id is present so we'll coerce.
            parsed_rust_message.pop("event_hash", None)
            parsed_python_message.pop("event_hash", None)

            # The python message is a subset of the rust message which contains the complete
            # row definition. This is due to a defect in the python processor. We take the
            # rust message and overly the python message. This fill in the gaps of the python
            # message.
            assert parsed_rust_message | parsed_python_message == parsed_rust_message


@patch("snuba.settings.DEFAULT_RETENTION_DAYS", 100)
@patch("snuba.settings.VALID_RETENTION_DAYS", {30, 60, 100})
def test_rust_compat_processor_honors_retention_settings() -> None:
    """Python retention settings must reach RustCompatProcessor via process_message."""
    example = next(
        data
        for ex in sentry_kafka_schemas.iter_examples("events")
        if isinstance((data := ex.load()), list) and len(data) >= 3 and data[1] == "insert"
    )
    example[2]["retention_days"] = 100

    processed = ErrorsProcessor().process_message(
        example,
        KafkaMessageMetadata(
            offset=1,
            partition=0,
            timestamp=datetime.utcfromtimestamp(int(time.time())),
        ),
    )

    assert isinstance(processed, InsertBatch)
    assert processed.rows
    assert processed.rows[0]["retention_days"] == 100


def test_eap_items_received_at_from_broker_timestamp() -> None:
    payload = next(iter(sentry_kafka_schemas.iter_examples("snuba-items"))).load()
    broker_timestamp = datetime.fromtimestamp(1_745_562_493.123, tz=UTC)

    with override_options("snuba", {"eap_items_emit_received_at": True}):
        processed = EAPItemsProcessor().process_message(
            payload,
            KafkaMessageMetadata(offset=1, partition=0, timestamp=broker_timestamp),
        )

    assert isinstance(processed, InsertBatch)
    assert processed.rows[0]["received_at"] == 1_745_562_493_123
