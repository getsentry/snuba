import uuid
from datetime import datetime
from typing import Optional

from snuba import settings
from snuba.processor import (
    InsertBatch,
    MessageProcessor,
    ProcessedMessage,
    _ensure_valid_date,
    _unicodify,
)


class OutcomesProcessor(MessageProcessor):
    def process_message(self, value, metadata=None) -> Optional[ProcessedMessage]:
        assert isinstance(value, dict)

        if value.get("category") in ("transaction", "attachment", "session"):
            return None

        v_uuid = value.get("event_id")
        message = {
            "org_id": value.get("org_id", 0),
            "project_id": value.get("project_id", 0),
            "key_id": value.get("key_id"),
            "timestamp": _ensure_valid_date(
                datetime.strptime(value["timestamp"], settings.PAYLOAD_DATETIME_FORMAT),
            ),
            "outcome": value["outcome"],
            "reason": _unicodify(value.get("reason")),
            "event_id": str(uuid.UUID(v_uuid)) if v_uuid is not None else None,
        }

        return InsertBatch([message])
