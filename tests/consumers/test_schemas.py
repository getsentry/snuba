from dataclasses import dataclass
from datetime import datetime
from typing import Any, Iterator, Optional

import pytest
import sentry_kafka_schemas
from sentry_kafka_schemas.types import Example

from snuba.consumers.types import KafkaMessageMetadata
from snuba.datasets.storages.factory import (
    get_writable_storage,
    get_writable_storage_keys,
    get_writable_storages,
)
from snuba.processor import MessageProcessor, ReplacementBatch
from snuba.replacers.replacer_processor import (
    ReplacementMessage,
    ReplacementMessageMetadata,
    ReplacerProcessor,
)


@dataclass
class Case:
    example: Example
    processor: MessageProcessor
    replacer_processor: Optional[ReplacerProcessor[Any]]

    def __repr__(self) -> str:
        return repr(self.example)


def _generate_tests() -> Iterator[Case]:
    for storage_key in get_writable_storage_keys():
        storage = get_writable_storage(storage_key)
        table_writer = storage.get_table_writer()
        stream_loader = table_writer.get_stream_loader()
        topic = stream_loader.get_default_topic_spec().topic

        processor = stream_loader.get_processor()
        replacer_processor = table_writer.get_replacer_processor()

        try:
            for example in sentry_kafka_schemas.iter_examples(topic.value):
                yield Case(
                    example=example,
                    processor=processor,
                    replacer_processor=replacer_processor,
                )
        except sentry_kafka_schemas.SchemaNotFound:
            pass


@pytest.mark.parametrize("case", _generate_tests())
def test_all_schemas(case: Case) -> None:
    """
    "Assert" that no message processor crashes under the example payloads in
    sentry-kafka-schemas
    """

    metadata = KafkaMessageMetadata(offset=1, partition=1, timestamp=datetime.now())
    result = case.processor.process_message(case.example.load(), metadata)

    if isinstance(result, ReplacementBatch):
        assert case.replacer_processor
        for message in result.values:
            [version, action_type, data] = message
            assert version == 2

            replacement_metadata = ReplacementMessageMetadata(
                partition_index=1, offset=1, consumer_group=""
            )
            case.replacer_processor.process_message(
                ReplacementMessage(
                    action_type=action_type, data=data, metadata=replacement_metadata
                )
            )


TEMPORARILY_SKIPPED_TOPICS = [
    "ingest-sessions",
    "cdc",
    "profiles-call-tree",
    "processed-profiles",
]


def test_has_kafka_schema() -> None:
    """
    Source topics for a writable storage must have schema defined.
    Temporarily skipped for a few topics where schemas are in progress.
    """
    for storage in get_writable_storages():
        stream_loader = storage.get_table_writer().get_stream_loader()
        topic_name = stream_loader.get_default_topic_spec().topic.value
        try:
            sentry_kafka_schemas.get_codec(topic_name)
        except sentry_kafka_schemas.SchemaNotFound:
            if topic_name in TEMPORARILY_SKIPPED_TOPICS:
                print("Temporarily skipped validation for topic: %s" % topic_name)
            else:
                raise
