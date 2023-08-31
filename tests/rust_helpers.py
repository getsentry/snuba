from typing import Any, Optional

from snuba.consumers.types import KafkaMessageMetadata
from snuba.datasets.processors import DatasetMessageProcessor
from snuba.processor import InsertBatch, ProcessedMessage


# A dummy processor for testing Rust strategies that converts a message into a
# single row to insert.
class IdentityProcessor(DatasetMessageProcessor):
    def process_message(
        self, message: Any, metadata: KafkaMessageMetadata
    ) -> Optional[ProcessedMessage]:
        return InsertBatch([message], None, None)
