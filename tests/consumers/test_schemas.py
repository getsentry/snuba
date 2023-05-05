from dataclasses import dataclass
from datetime import datetime
from typing import Any, Iterator, Optional

import pytest
import sentry_kafka_schemas
from sentry_kafka_schemas.types import Example
from sentry_kafka_schemas.sentry_kafka_schemas import _get_schema

from snuba.consumers.types import KafkaMessageMetadata
from snuba.datasets.storages.factory import (
    get_writable_storage,
    get_writable_storage_keys,
)
from snuba.processor import MessageProcessor, ReplacementBatch
from snuba.replacers.replacer_processor import (
    ReplacementMessage,
    ReplacementMessageMetadata,
    ReplacerProcessor,
)

@dataclass
class TopicConfig:
    logical_topic_name: str
    processor: MessageProcessor
    replacer_processor: Optional[ReplacerProcessor[Any]]

@dataclass
class Case:
    example: Any
    config: TopicConfig

    def __repr__(self) -> str:
        return repr(self.example)


def _generate_topic_configs() -> Iterator[TopicConfig]:
    for storage_key in get_writable_storage_keys():
        storage = get_writable_storage(storage_key)
        table_writer = storage.get_table_writer()
        stream_loader = table_writer.get_stream_loader()
        topic = stream_loader.get_default_topic_spec().topic

        processor = stream_loader.get_processor()
        replacer_processor = table_writer.get_replacer_processor()
        try:
            sentry_kafka_schemas.get_codec(topic.value)
        except sentry_kafka_schemas.SchemaNotFound:
            continue

        yield TopicConfig(processor=processor, replacer_processor=replacer_processor, logical_topic_name=topic.value)



from hypothesis import given, settings
from hypothesis_jsonschema import from_schema

@pytest.mark.parametrize("config", _generate_topic_configs())
def test_fuzz_schemas(config: TopicConfig):
    schema = _get_schema(config.logical_topic_name)['schema']

    @given(value=from_schema(schema))
    @settings(max_examples=1)
    def inner(value):
        run_test(Case(config=config, example=value))


def _generate_tests() -> Iterator[Case]:
    for config in _generate_topic_configs():
        for example in sentry_kafka_schemas.iter_examples(config.logical_topic_name):
            yield Case(
                config=config,
                example=example.load(),
            )

@pytest.mark.parametrize("case", _generate_tests())
def test_all_schemas(case: Case) -> None:
    """
    "Assert" that no message processor crashes under the example payloads in
    sentry-kafka-schemas
    """

    run_test(case)


def run_test(case: Case) -> None:
    metadata = KafkaMessageMetadata(offset=1, partition=1, timestamp=datetime.now())
    result = case.config.processor.process_message(case.example, metadata)

    if isinstance(result, ReplacementBatch):
        assert case.config.replacer_processor
        for message in result.values:
            [version, action_type, data] = message
            assert version == 2

            replacement_metadata = ReplacementMessageMetadata(
                partition_index=1, offset=1, consumer_group=""
            )
            case.config.replacer_processor.process_message(
                ReplacementMessage(
                    action_type=action_type, data=data, metadata=replacement_metadata
                )
            )
