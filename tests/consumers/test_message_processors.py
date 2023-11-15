import json
import time
from datetime import datetime
from typing import Type

import pytest
import rust_snuba
import sentry_kafka_schemas

from snuba.consumers.types import KafkaMessageMetadata
from snuba.datasets.processors import DatasetMessageProcessor
from snuba.datasets.processors.functions_processor import FunctionsMessageProcessor
from snuba.datasets.processors.profiles_processor import ProfilesMessageProcessor
from snuba.datasets.processors.querylog_processor import QuerylogProcessor
from snuba.datasets.processors.spans_processor import SpansMessageProcessor
from snuba.processor import InsertBatch


@pytest.mark.parametrize(
    "topic,processor",
    [
        ("processed-profiles", ProfilesMessageProcessor),
        ("profiles-call-tree", FunctionsMessageProcessor),
        ("snuba-queries", QuerylogProcessor),
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

        rust_processed_message = rust_snuba.process_message(  # type: ignore
            processor_name, data_bytes, partition, offset, millis_since_epoch
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

        assert (
            json.loads(bytes(rust_processed_message))
            == python_processed_message.rows[0]
        )
