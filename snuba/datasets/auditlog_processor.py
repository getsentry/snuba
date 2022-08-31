import datetime
from typing import Any, Mapping, Optional
from uuid import UUID

from snuba.consumers.types import KafkaMessageMetadata
from snuba.processor import InsertBatch, MessageProcessor, ProcessedMessage

# metrics = MetricsWrapper(environment.metrics, "snuba.querylog")


class AuditlogProcessor(MessageProcessor):
    def process_message(
        self, message: Mapping[str, Any], metadata: KafkaMessageMetadata
    ) -> Optional[ProcessedMessage]:
        try:
            processed = {
                "timestamp": datetime.datetime.utcfromtimestamp(message["timestamp"]),
                "event_type": str(message["event_type"]),
                "user": str(message["user"]),
                "details": str(message["details"]),
                "event_id": UUID(message["event_id"]),
            }
        except ValueError:
            # metrics.increment("invalid_uuid")
            return None
        except KeyError:
            # metrics.increment("missing_field")
            return None
        return InsertBatch([processed], None)
