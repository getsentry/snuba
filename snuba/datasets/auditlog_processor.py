from datetime import datetime
from typing import Any, Mapping, Optional
from uuid import UUID

from snuba import environment
from snuba.consumers.types import KafkaMessageMetadata
from snuba.processor import InsertBatch, MessageProcessor, ProcessedMessage
from snuba.utils.metrics.wrapper import MetricsWrapper

metrics = MetricsWrapper(environment.metrics, "snuba.auditlog")


class AuditlogProcessor(MessageProcessor):
    def process_message(
        self, message: Mapping[str, Any], metadata: KafkaMessageMetadata
    ) -> Optional[ProcessedMessage]:
        try:
            processed = {
                "event_type": message["event_type"],
                "user": message["user"],
                "details": message["details"],
                "event_id": str(UUID(message["event_id"])),
                "timestamp": datetime.utcfromtimestamp(message["timestamp"]),
                "project_id": int(message["project_id"]),
                "offset": metadata.offset,
            }
        except ValueError as e:
            print("valuue error", e)
            return None
        return InsertBatch([processed], None)
