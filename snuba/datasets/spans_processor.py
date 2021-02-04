import uuid
from datetime import datetime
from typing import Any, List, Mapping, MutableMapping, Optional, Sequence, Tuple

from sentry_relay.consts import SPAN_STATUS_NAME_TO_CODE

from snuba import environment
from snuba.consumers.types import KafkaMessageMetadata
from snuba.datasets.events_format import enforce_retention, extract_extra_tags
from snuba.processor import (
    InsertBatch,
    MessageProcessor,
    ProcessedMessage,
    _as_dict_safe,
    _ensure_valid_date,
    _unicodify,
)
from snuba.utils.metrics.wrapper import MetricsWrapper

UNKNOWN_SPAN_STATUS = 2

metrics = MetricsWrapper(environment.metrics, "spans.processor")


class SpansMessageProcessor(MessageProcessor):
    def __extract_timestamp(self, field: float) -> Tuple[datetime, int]:
        timestamp = _ensure_valid_date(datetime.utcfromtimestamp(field))
        if timestamp is None:
            timestamp = datetime.utcnow()
        nanoseconds = int(timestamp.microsecond * 1000)
        return (timestamp, nanoseconds)

    def __init_span(self, event: Mapping[str, Any]) -> MutableMapping[str, Any]:
        """
        Initializes the fields that are the same for all spans within a transaction.
        """
        data = event["data"]
        transaction_ctx = data["contexts"]["trace"]

        return {
            "deleted": 0,
            "project_id": event["project_id"],
            "transaction_id": str(uuid.UUID(event["event_id"])),
            "retention_days": enforce_retention(
                event, datetime.utcfromtimestamp(data["timestamp"])
            ),
            "transaction_span_id": int(transaction_ctx["span_id"], 16),
            "trace_id": str(uuid.UUID(transaction_ctx["trace_id"])),
            "transaction_name": _unicodify(data.get("transaction") or ""),
        }

    def __fill_status(
        self, span: MutableMapping[str, Any], status: Optional[str]
    ) -> None:
        if status:
            int_status = SPAN_STATUS_NAME_TO_CODE.get(status, UNKNOWN_SPAN_STATUS)
        else:
            int_status = UNKNOWN_SPAN_STATUS
        span["status"] = int_status

    def __fill_common(
        self, span: MutableMapping[str, Any], data: Mapping[str, Any]
    ) -> None:
        """
        Fills in the fields that have the same structure between transactions and spans
        but that come from a different dictionary.
        """
        span["start_ts"], span["start_ns"] = self.__extract_timestamp(
            data["start_timestamp"],
        )
        span["finish_ts"], span["finish_ns"] = self.__extract_timestamp(
            data["timestamp"],
        )
        duration_secs = (span["finish_ts"] - span["start_ts"]).total_seconds()
        # duration is in milliseconds
        span["duration_ms"] = max(int(duration_secs * 1000), 0)

    def __safe_extract_int(
        self, field: str, value: Any, nullable: bool
    ) -> Optional[int]:
        if value is not None:
            if isinstance(value, str):
                return int(value, 16)
            elif isinstance(value, int):
                return value
            else:
                metrics.increment(
                    "invalid_int_type", tags={"field": field, "type": str(type(value))}
                )
        elif not nullable:
            metrics.increment("missing_field", tags={"field": field})
        return None

    def process_message(
        self, message: Sequence[Any], metadata: KafkaMessageMetadata
    ) -> Optional[ProcessedMessage]:
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

        # Add the transaction span
        transaction_ctx = data["contexts"].get("trace")
        if not transaction_ctx:
            metrics.increment("missing_trace_ctx")
            return None

        # Add the transaction root span
        processed = self.__init_span(event)
        processed["span_id"] = self.__safe_extract_int(
            "transaction:span_id", transaction_ctx["span_id"], False
        )
        if processed["span_id"] is None:
            return None
        processed["transaction_name"] = _unicodify(data.get("transaction") or "")
        processed["parent_span_id"] = self.__safe_extract_int(
            "transaction:parent_span_id", transaction_ctx.get("parent_span_id"), True
        )

        processed["op"] = _unicodify(transaction_ctx.get("op") or "")
        status = transaction_ctx.get("status", None)
        self.__fill_status(processed, status)
        self.__fill_common(processed, event["data"])
        ret.append(processed)

        spans = data.get("spans", [])
        for span in spans:
            processed = self.__init_span(event)
            processed["span_id"] = self.__safe_extract_int(
                "span:span_id", span["span_id"], False
            )
            if processed["span_id"] is None:
                return None
            processed["parent_span_id"] = self.__safe_extract_int(
                "span:parent_span_id", span.get("parent_span_id"), True
            )

            processed["op"] = span["op"]
            tags = _as_dict_safe(span.get("tags", None))
            processed["tags.key"], processed["tags.value"] = extract_extra_tags(tags)

            status = span.get("status", None)
            self.__fill_status(processed, status)
            self.__fill_common(processed, span)
            ret.append(processed)

        if ret:
            return InsertBatch(ret)
        else:
            return None
