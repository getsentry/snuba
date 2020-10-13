from datetime import datetime
from typing import MutableSequence, Sequence

from snuba.consumer import KafkaMessageMetadata
from snuba.clickhouse.http import JSONRowEncoder
from snuba.datasets.events_processor_base import InsertEvent
from snuba.datasets.storage import WritableStorage
from snuba.processor import InsertBatch, ProcessedMessage
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
    storage: WritableStorage, events: Sequence[InsertEvent]
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
