import uuid
from datetime import datetime
from typing import Any, List, MutableMapping, Optional, Tuple

from sentry_relay.consts import SPAN_STATUS_NAME_TO_CODE

from snuba.datasets.events_format import (
    enforce_retention,
    extract_extra_tags,
)
from snuba.processor import (
    InsertBatch,
    MessageProcessor,
    ProcessedMessage,
    _as_dict_safe,
    _ensure_valid_date,
    _unicodify,
)

UNKNOWN_SPAN_STATUS = 2


class SpansMessageProcessor(MessageProcessor):
    def __extract_timestamp(self, field: float) -> Tuple[datetime, int]:
        timestamp = _ensure_valid_date(datetime.fromtimestamp(field))
        if timestamp is None:
            timestamp = datetime.utcnow()
        nanoseconds = int(timestamp.microsecond * 1000)
        return (timestamp, nanoseconds)

    def process_message(self, message, metadata) -> Optional[ProcessedMessage]:
        if not (isinstance(message, (list, tuple)) and len(message) >= 2):
            return None
        version = message[0]
        if version not in (0, 1, 2):
            return None
        type_, event = message[1:3]
        if type_ != "insert":
            return None

        data = event["data"]
        event_type = data.get("type")
        if event_type != "transaction":
            return None

        ret: List[MutableMapping[str, Any]] = []
        retention = enforce_retention(event, datetime.fromtimestamp(data["timestamp"]))
        transaction_ctx = data["contexts"]["trace"]
        trace_id = str(uuid.UUID(transaction_ctx["trace_id"]))
        transaction_span_id = int(transaction_ctx["span_id"], 16)

        # Add the transaction span
        processed: MutableMapping[str, Any] = {"deleted": 0}
        processed["project_id"] = event["project_id"]
        processed["retention_days"] = retention
        processed["transaction_span_id"] = transaction_span_id
        processed["span_id"] = transaction_span_id
        processed["trace_id"] = trace_id
        processed["transaction_name"] = _unicodify(data.get("transaction") or "")
        processed["parent_span_id"] = (
            int(transaction_ctx["parent_span_id"], 16)
            if "parent_span_id" in transaction_ctx
            else 0
        )
        processed["description"] = _unicodify(data.get("transaction") or "")
        processed["op"] = _unicodify(transaction_ctx.get("op") or "")

        status = transaction_ctx.get("status", None)
        if status:
            int_status = SPAN_STATUS_NAME_TO_CODE.get(status, UNKNOWN_SPAN_STATUS)
        else:
            int_status = UNKNOWN_SPAN_STATUS
        processed["status"] = int_status

        data = event["data"]
        processed["start_ts"], processed["start_ns"] = self.__extract_timestamp(
            data["start_timestamp"],
        )
        processed["finish_ts"], processed["finish_ns"] = self.__extract_timestamp(
            data["timestamp"],
        )
        duration_secs = (processed["finish_ts"] - processed["start_ts"]).total_seconds()
        processed["duration"] = max(int(duration_secs * 1000), 0)

        tags = _as_dict_safe(data.get("tags", None))
        processed["tags.key"], processed["tags.value"] = extract_extra_tags(tags)
        ret.append(processed)

        spans = data.get("spans", [])
        for span in spans:
            processed: MutableMapping[str, Any] = {"deleted": 0}
            processed["project_id"] = event["project_id"]
            processed["retention_days"] = retention
            processed["transaction_span_id"] = transaction_span_id
            processed["span_id"] = int(span["span_id"], 16)
            processed["trace_id"] = trace_id
            processed["transaction_name"] = _unicodify(data.get("transaction") or "")
            processed["parent_span_id"] = int(span["parent_span_id"], 16)
            processed["description"] = span.get("description", "") or ""
            processed["op"] = span["op"]

            status = span.get("status", None)
            if status:
                int_status = SPAN_STATUS_NAME_TO_CODE.get(status, UNKNOWN_SPAN_STATUS)
            else:
                int_status = UNKNOWN_SPAN_STATUS
            processed["status"] = int_status

            processed["start_ts"], processed["start_ns"] = self.__extract_timestamp(
                span["start_timestamp"],
            )
            processed["finish_ts"], processed["finish_ns"] = self.__extract_timestamp(
                span["timestamp"],
            )
            duration_secs = (
                processed["finish_ts"] - processed["start_ts"]
            ).total_seconds()
            processed["duration"] = max(int(duration_secs * 1000), 0)

            tags = _as_dict_safe(span.get("tags", None))
            processed["tags.key"], processed["tags.value"] = extract_extra_tags(tags)
            ret.append(processed)

        if ret:
            return InsertBatch(ret)
        else:
            return None
