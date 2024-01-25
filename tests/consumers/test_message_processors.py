from __future__ import annotations

import json
import time
from datetime import datetime
from typing import Type
from unittest.mock import patch

import pytest
import rust_snuba
import sentry_kafka_schemas

from snuba.consumers.types import KafkaMessageMetadata
from snuba.datasets.processors import DatasetMessageProcessor
from snuba.datasets.processors.errors_processor import ErrorsProcessor
from snuba.datasets.processors.generic_metrics_processor import (
    GenericCountersMetricsProcessor,
    GenericDistributionsMetricsProcessor,
    GenericGaugesMetricsProcessor,
    GenericSetsMetricsProcessor,
)
from snuba.datasets.processors.outcomes_processor import OutcomesProcessor
from snuba.datasets.processors.replays_processor import ReplaysProcessor
from snuba.processor import InsertBatch


@pytest.mark.parametrize(
    "topic,processor",
    [
        ("events", ErrorsProcessor),
        ("outcomes", OutcomesProcessor),
        ("outcomes", OutcomesProcessor),
        ("snuba-generic-metrics", GenericCountersMetricsProcessor),
        ("snuba-generic-metrics", GenericSetsMetricsProcessor),
        ("snuba-generic-metrics", GenericDistributionsMetricsProcessor),
        ("snuba-generic-metrics", GenericGaugesMetricsProcessor),
    ],
)
@patch("snuba.settings.DISCARD_OLD_EVENTS", False)
def test_message_processors(
    topic: str, processor: Type[DatasetMessageProcessor]
) -> None:
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
        elif topic == "snuba-spans":
            data_json["start_timestamp_ms"] = int(time.time()) * 1000
            data_json.pop("group_raw", None)

        data_bytes = json.dumps(data_json).encode("utf-8")

        processor_name = processor.__qualname__
        partition = 0
        offset = 1
        millis_since_epoch = int(time.time() * 1000)

        rust_processed_message = bytes(
            rust_snuba.process_message(  # type: ignore
                processor_name, data_bytes, partition, offset, millis_since_epoch
            )
        )
        python_processed_message = processor().process_message(
            data_json,
            KafkaMessageMetadata(
                offset=offset,
                partition=partition,
                timestamp=datetime.utcfromtimestamp(millis_since_epoch / 1000),
            ),
        )

        # Handle scenarios where the message needs to be skipped by the processor
        if not python_processed_message:
            assert rust_processed_message == b""
            continue

        assert isinstance(python_processed_message, InsertBatch)

        assert [
            json.loads(line)
            for line in rust_processed_message.rstrip(b"\n").split(b"\n")
            if line
        ] == python_processed_message.rows


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
            rust_snuba.process_message(  # type: ignore
                processor_name, data_bytes, partition, offset, millis_since_epoch
            )
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
            parsed_python_message = python_processed_message.rows[0]

            # timestamp is sometimes in different formats so we'll coerce.
            ts1 = parsed_rust_message.pop("timestamp", None)
            ts2 = parsed_python_message.pop("timestamp", None)  # type: ignore
            if isinstance(ts2, datetime):
                ts2 = int(ts2.timestamp())
            assert ts1 == ts2

            # replay_start_timestamp is sometimes in different formats so we'll coerce.
            sts1 = parsed_rust_message.pop("replay_start_timestamp", None)
            sts2 = parsed_python_message.pop("replay_start_timestamp", None)  # type: ignore
            if isinstance(sts2, datetime):
                sts2 = int(sts2.timestamp())
            assert sts1 == sts2

            # event_hash is generated by the consumer and not always consistent if
            # no segment_id is present so we'll coerce.
            parsed_rust_message.pop("event_hash", None)
            parsed_python_message.pop("event_hash", None)  # type: ignore

            # The python message is a subset of the rust message which contains the complete
            # row definition. This is due to a defect in the python processor. We take the
            # rust message and overly the python message. This fill in the gaps of the python
            # message.
            assert parsed_rust_message | parsed_python_message == parsed_rust_message
