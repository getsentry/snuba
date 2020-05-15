import uuid
from datetime import datetime
from typing import Optional

from snuba.processor import (
    MAX_UINT32,
    MessageProcessor,
    NIL_UUID,
    ProcessedMessage,
    ProcessorAction,
    _collapse_uint16,
    _collapse_uint32,
    _ensure_valid_date,
)

STATUS_MAPPING = {
    "ok": 0,
    "exited": 1,
    "crashed": 2,
    "abnormal": 3,
}


class SessionsProcessor(MessageProcessor):
    def process_message(self, message, metadata=None) -> Optional[ProcessedMessage]:
        # some old relays accidentally emit rows without release
        if message["release"] is None:
            return None
        if message["duration"] is None:
            duration = None
        else:
            duration = _collapse_uint32(int(message["duration"] * 1000))

        # since duration is not nullable, the max duration means no duration
        if duration is None:
            duration = MAX_UINT32

        errors = _collapse_uint16(message["errors"]) or 0

        # If a session ends in crashed or abnormal we want to make sure that
        # they count as errored too, so we can get the number of health and
        # errored sessions correctly.
        if message["status"] in ("crashed", "abnormal"):
            errors = max(errors, 1)

        processed = {
            "session_id": str(uuid.UUID(message["session_id"])),
            "distinct_id": str(uuid.UUID(message.get("distinct_id") or NIL_UUID)),
            "seq": message["seq"],
            "org_id": message["org_id"],
            "project_id": message["project_id"],
            "retention_days": message["retention_days"],
            "duration": duration,
            "status": STATUS_MAPPING[message["status"]],
            "errors": errors,
            "received": _ensure_valid_date(
                datetime.utcfromtimestamp(message["received"])
            ),
            "started": _ensure_valid_date(
                datetime.utcfromtimestamp(message["started"])
            ),
            "release": message["release"],
            "environment": message.get("environment") or "",
        }
        return ProcessedMessage(action=ProcessorAction.INSERT, data=[processed])
