from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from typing import Any, Type

import pytest
import rust_snuba
import sentry_kafka_schemas

from snuba.consumers.types import KafkaMessageMetadata
from snuba.datasets.processors import DatasetMessageProcessor
from snuba.datasets.processors.functions_processor import FunctionsMessageProcessor
from snuba.datasets.processors.metrics_summaries_processor import (
    MetricsSummariesMessageProcessor,
)
from snuba.datasets.processors.profiles_processor import ProfilesMessageProcessor
from snuba.datasets.processors.querylog_processor import QuerylogProcessor
from snuba.datasets.processors.replays_processor import ReplaysProcessor
from snuba.datasets.processors.spans_processor import SpansMessageProcessor
from snuba.processor import InsertBatch


@pytest.mark.parametrize(
    "topic,processor",
    [
        ("processed-profiles", ProfilesMessageProcessor),
        ("profiles-call-tree", FunctionsMessageProcessor),
        ("snuba-queries", QuerylogProcessor),
        ("snuba-spans", MetricsSummariesMessageProcessor),
        ("snuba-spans", SpansMessageProcessor),
    ],
)
def test_message_processors(
    topic: str, processor: Type[DatasetMessageProcessor]
) -> None:
    """
    Tests the output of Python and Rust message processors is the same
    """
    for ex in sentry_kafka_schemas.iter_examples(topic):
        data_json = ex.load()
        # Hacks to ensure the message isn't rejected with too old
        if topic == "processed-profiles":
            data_json["received"] = int(time.time())
        elif topic == "snuba-spans":
            data_json["start_timestamp_ms"] = int(time.time()) * 1000

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

        assert [
            json.loads(line)
            for line in rust_processed_message.rstrip(b"\n").split(b"\n")
            if line
        ] == python_processed_message.rows


def test_replay_processor() -> None:
    """Tests the output of the two replays message processors are the same."""

    # Rows created by the python processor and different from the rows created by the rust
    # processor. These differences are superficial and have no production impact.
    #
    # For example, the rust version preserves milliseconds when parsing. However, after
    # inserting into the database this precision is dropped.
    def normalize_row(row: dict[str, Any]) -> dict[str, Any]:
        row["timestamp"] = datetime.fromtimestamp(
            int(row["timestamp"]), tz=timezone.utc
        )
        if row.get("replay_start_timestamp"):
            row["replay_start_timestamp"] = datetime.fromtimestamp(
                int(row["replay_start_timestamp"]), tz=timezone.utc
            )
        return row

    for ex in sentry_kafka_schemas.iter_examples("ingest-replay-events"):
        data_json = ex.load()
        data_json["start_time"] = int(time.time())
        data_bytes = json.dumps(data_json).encode("utf-8")

        processor_name = ReplaysProcessor.__qualname__
        partition = 0
        offset = 1
        millis_since_epoch = int(time.time() * 1000)

        rust_processed_message = bytes(
            rust_snuba.process_message(  # type: ignore
                processor_name, data_bytes, partition, offset, millis_since_epoch
            )
        )
        python_processed_message = ReplaysProcessor().process_message(
            data_json,
            KafkaMessageMetadata(
                offset=offset,
                partition=partition,
                timestamp=datetime.utcfromtimestamp(millis_since_epoch / 1000),
            ),
        )

        assert isinstance(python_processed_message, InsertBatch)

        assert [
            normalize_row(json.loads(line))
            for line in rust_processed_message.rstrip(b"\n").split(b"\n")
            if line
        ] == python_processed_message.rows
