import datetime
from typing import Any, Mapping, Optional
from uuid import UUID

from snuba.consumers.types import KafkaMessageMetadata
from snuba.processor import InsertBatch, MessageProcessor, ProcessedMessage

# metrics = MetricsWrapper(Environment.metrics, "snuba.querylog")


class AuditlogProcessor(MessageProcessor):
    def process_message(
        self, message: Mapping[str, Any], metadata: KafkaMessageMetadata
    ) -> Optional[ProcessedMessage]:

        processed = {
            "timestamp": datetime.datetime.utcfromtimestamp(message["timestamp"]),
            "event_type": str(message["event_type"]),
            "user": str(message["user"]),
            "details": str(message["details"]),
            "event_id": str(UUID(message["event_id"])),
        }

        return InsertBatch([processed], None)
