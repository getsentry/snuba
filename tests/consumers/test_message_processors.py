from __future__ import annotations

import json
import time
from datetime import datetime
from typing import Type

import pytest
import rust_snuba
import sentry_kafka_schemas

from snuba.consumers.types import KafkaMessageMetadata
from snuba.datasets.processors import DatasetMessageProcessor
from snuba.datasets.processors.outcomes_processor import OutcomesProcessor
from snuba.processor import InsertBatch


@pytest.mark.parametrize(
    "topic,processor",
    [
        # TODO(colton): See why the replays tests are failing with the latest
        # examples from sentry-kafka-schemas.
        # ("ingest-replay-events", ReplaysProcessor),
        ("outcomes", OutcomesProcessor),
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

        assert isinstance(python_processed_message, InsertBatch)

        assert [
            json.loads(line)[0] if topic == "ingest-replay-events" else json.loads(line)
            for line in rust_processed_message.rstrip(b"\n").split(b"\n")
            if line
        ] == python_processed_message.rows
