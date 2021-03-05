import uuid
from datetime import datetime
from typing import Any, Mapping, Optional

from snuba.consumers.types import KafkaMessageMetadata
from sentry_relay import DataCategory

from snuba import settings, environment
from snuba.processor import (
    InsertBatch,
    MessageProcessor,
    ProcessedMessage,
    _ensure_valid_date,
    _unicodify,
)

from snuba.utils.metrics.wrapper import MetricsWrapper

metrics = MetricsWrapper(environment.metrics, "outcomes.processor")


class OutcomesProcessor(MessageProcessor):
    def process_message(
        self, value: Mapping[str, Any], metadata: KafkaMessageMetadata
    ) -> Optional[ProcessedMessage]:
        assert isinstance(value, dict)
        v_uuid = value.get("event_id")

        if "category" not in value:
            metrics.increment("missing_category")

        message = {
            "org_id": value.get("org_id", 0),
            "project_id": value.get("project_id", 0),
            "key_id": value.get("key_id"),
            "timestamp": _ensure_valid_date(
                datetime.strptime(value["timestamp"], settings.PAYLOAD_DATETIME_FORMAT),
            ),
            "outcome": value["outcome"],
            "category": value.get("category", DataCategory.ERROR),
            "quantity": value.get("quantity", 1),
            "reason": _unicodify(value.get("reason")),
            "event_id": str(uuid.UUID(v_uuid)) if v_uuid is not None else None,
        }

        return InsertBatch([message], None)
