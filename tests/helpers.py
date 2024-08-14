from datetime import datetime
from typing import Any, Mapping, MutableSequence, Sequence, Union

from snuba.clickhouse.http import JSONRowEncoder
from snuba.consumers.types import KafkaMessageMetadata
from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity, override_entity_map
from snuba.datasets.pluggable_entity import PluggableEntity
from snuba.datasets.storage import WritableStorage
from snuba.processor import InsertBatch, InsertEvent, ProcessedMessage
from snuba.query.validation.validators import ColumnValidationMode
from snuba.utils.metrics.backends.dummy import DummyMetricsBackend
from snuba.writer import BatchWriterEncoderWrapper, WriterTableRow


def write_processed_messages(
    storage: WritableStorage, messages: Sequence[ProcessedMessage]
) -> None:
    rows: MutableSequence[WriterTableRow] = []
    for message in messages:
        assert isinstance(message, InsertBatch)
        rows.extend(message.rows)

    BatchWriterEncoderWrapper(
        storage.get_table_writer().get_batch_writer(
            metrics=DummyMetricsBackend(strict=True)
        ),
        JSONRowEncoder(),
    ).write(rows)


def write_unprocessed_events(
    storage: WritableStorage, events: Sequence[Union[InsertEvent, Mapping[str, Any]]]
) -> None:
    processor = storage.get_table_writer().get_stream_loader().get_processor()
    processed_messages = []
    for i, event in enumerate(events):
        processed_message = processor.process_message(
            (2, "insert", event, {}), KafkaMessageMetadata(i, 0, datetime.now())
        )
        assert processed_message is not None
        processed_messages.append(processed_message)

    write_processed_messages(storage, processed_messages)


def write_raw_unprocessed_events(
    storage: WritableStorage,
    events: Sequence[Union[InsertEvent, Mapping[str, Any]]],
) -> None:
    processor = storage.get_table_writer().get_stream_loader().get_processor()

    processed_messages = []
    for i, event in enumerate(events):
        processed_message = processor.process_message(
            event, KafkaMessageMetadata(i, 0, datetime.now())
        )
        assert processed_message is not None
        processed_messages.append(processed_message)

    write_processed_messages(storage, processed_messages)


def override_entity_column_validator(
    entity_key: EntityKey, validator_mode: ColumnValidationMode
) -> None:
    entity = get_entity(entity_key)
    assert isinstance(entity, PluggableEntity)
    entity.validate_data_model = validator_mode
    override_entity_map(entity_key, entity)
