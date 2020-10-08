import os
from datetime import datetime
from typing import MutableSequence, Optional, Sequence

from snuba.clickhouse.http import JSONRowEncoder
from snuba.consumer import KafkaMessageMetadata
from snuba.datasets.events_processor_base import InsertEvent
from snuba.datasets.factory import enforce_table_writer, get_dataset
from snuba.processor import InsertBatch, ProcessedMessage
from snuba.utils.metrics.backends.dummy import DummyMetricsBackend
from snuba.writer import BatchWriterEncoderWrapper, WriterTableRow


class BaseDatasetTest:
    def setup_method(self, test_method, dataset_name: Optional[str] = None):
        self.database = os.environ.get("CLICKHOUSE_DATABASE", "default")
        self.dataset_name = dataset_name

        if dataset_name is not None:
            self.dataset = get_dataset(dataset_name)
        else:
            self.dataset = None

    def write_processed_messages(self, messages: Sequence[ProcessedMessage]) -> None:
        rows: MutableSequence[WriterTableRow] = []
        for message in messages:
            assert isinstance(message, InsertBatch)
            rows.extend(message.rows)
        self.write_rows(rows)

    def write_rows(self, rows: Sequence[WriterTableRow]) -> None:
        BatchWriterEncoderWrapper(
            enforce_table_writer(self.dataset).get_batch_writer(
                metrics=DummyMetricsBackend(strict=True)
            ),
            JSONRowEncoder(),
        ).write(rows)

    def write_events(self, events: Sequence[InsertEvent]) -> None:
        processor = (
            enforce_table_writer(self.dataset).get_stream_loader().get_processor()
        )

        processed_messages = []
        for i, event in enumerate(events):
            processed_message = processor.process_message(
                (2, "insert", event, {}), KafkaMessageMetadata(i, 0, datetime.now())
            )
            assert processed_message is not None
            processed_messages.append(processed_message)

        self.write_processed_messages(processed_messages)


class BaseApiTest(BaseDatasetTest):
    def setup_method(self, test_method, dataset_name="events"):
        super().setup_method(test_method, dataset_name)
        from snuba.web.views import application

        assert application.testing is True
        application.config["PROPAGATE_EXCEPTIONS"] = False
        self.app = application.test_client()
