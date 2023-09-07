import json
import time
from datetime import datetime

import rust_snuba
import sentry_kafka_schemas

from snuba.consumers.types import KafkaMessageMetadata
from snuba.datasets.processors.querylog_processor import QuerylogProcessor
from snuba.processor import InsertBatch


def test_message_processors() -> None:
    """
    Tests the output of Python and Rust message processors is the same
    """
    topic = "snuba-queries"
    processor_name = "QuerylogProcessor"

    for ex in sentry_kafka_schemas.iter_examples(topic):
        data_json = ex.load()
        data_bytes = json.dumps(data_json).encode("utf-8")

        partition = 0
        offset = 1
        millis_since_epoch = int(time.time() * 1000)

        rust_processed_message = rust_snuba.process_message(  # type: ignore
            processor_name, data_bytes, partition, offset, millis_since_epoch
        )
        python_processed_message = QuerylogProcessor().process_message(
            data_json,
            KafkaMessageMetadata(
                partition, offset, datetime.utcfromtimestamp(millis_since_epoch / 1000)
            ),
        )
        assert isinstance(python_processed_message, InsertBatch)

        assert (
            json.loads(bytes(rust_processed_message))
            == python_processed_message.rows[0]
        )
