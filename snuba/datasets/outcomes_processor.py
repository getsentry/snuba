import uuid
from datetime import datetime
from typing import Optional

from sentry_relay import DataCategory

from snuba import settings
from snuba.processor import (
    InsertBatch,
    MessageProcessor,
    ProcessedMessage,
    _ensure_valid_date,
    _unicodify,
)


class OutcomesProcessor(MessageProcessor):
    def process_message(self, value, metadata) -> Optional[ProcessedMessage]:
        assert isinstance(value, dict)
        # TODO: validation around size and category?
        v_uuid = value.get("event_id")
        message = {
            "org_id": value.get("org_id", 0),
            "project_id": value.get("project_id", 0),
            "key_id": value.get("key_id"),
            "timestamp": _ensure_valid_date(
                datetime.strptime(value["timestamp"], settings.PAYLOAD_DATETIME_FORMAT),
            ),
            "category": value.get(
                "category", DataCategory.ERROR
            ).value,  # if category is None, default to error for now TODO: change security to error?
            "size": value.get(
                "size", 1
            ),  # should probably be a default value of 1 for size and none
            "outcome": value.get("outcome", None),
            "reason": _unicodify(value.get("reason")),
            "event_id": str(uuid.UUID(v_uuid)) if v_uuid is not None else None,
        }

        return InsertBatch([message])
