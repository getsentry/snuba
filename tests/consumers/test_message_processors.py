import json

import rust_snuba
import sentry_kafka_schemas

from snuba.datasets.processors.querylog_processor import QuerylogProcessor


def test_message_processors() -> None:
    """
    Tests the output of Python and Rust message processors is the same
    """
    topic = "snuba-queries"
    processor_name = "QuerylogProcessor"

    for ex in sentry_kafka_schemas.iter_examples(topic):
        data_json = ex.load()
        data_bytes = json.dumps(data_json).encode("utf-8")

        rust_snuba.process_message(processor_name, data_bytes)

    # processed_message_python = QuerylogProcessor().process_message()
