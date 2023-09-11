import logging
import numbers
import random
import uuid
from datetime import datetime
from typing import Any, Mapping, MutableMapping, MutableSequence, Optional, Tuple

from sentry_kafka_schemas.schema_types.snuba_spans_v1 import SpanEvent
from sentry_relay.consts import SPAN_STATUS_NAME_TO_CODE

from snuba import environment, state
from snuba.consumers.types import KafkaMessageMetadata
from snuba.datasets.events_format import (
    EventTooOld,
    enforce_retention,
    extract_extra_tags,
    extract_nested,
)
from snuba.datasets.processors import DatasetMessageProcessor
from snuba.processor import (
    InsertBatch,
    ProcessedMessage,
    _as_dict_safe,
    _ensure_valid_date,
    _unicodify,
)
from snuba.utils.metrics.wrapper import MetricsWrapper

logger = logging.getLogger(__name__)

metrics = MetricsWrapper(environment.metrics, "spans.processor")

UNKNOWN_SPAN_STATUS = 2

EventDict = MutableMapping[str, Any]
SpanDict = MutableMapping[str, Any]
CommonSpanDict = MutableMapping[str, Any]
RetentionDays = int


class SpansMessageProcessor(DatasetMessageProcessor):
    """
    Message processor for writing spans data to the spans table.
    The implementation has taken inspiration from the transactions processor.
    The initial version of this processor is able to read existing data
    from the transactions topic and de-normalize it into the spans table.
    """

    def __extract_timestamp(self, timestamp_sec: float) -> Tuple[datetime, int]:
        # We are purposely using a naive datetime here to work with the rest of the codebase.
        # We can be confident that clients are only sending UTC dates.
        timestamp = _ensure_valid_date(datetime.utcfromtimestamp(timestamp_sec))
        if timestamp is None:
            timestamp = datetime.utcnow()
        milliseconds = int(timestamp.microsecond / 1000)
        return timestamp, milliseconds

    @staticmethod
    def _structure_and_validate_message(
        message: SpanEvent,
    ) -> Optional[Tuple[SpanEvent, RetentionDays]]:
        if not message.get("trace_id"):
            return None
        try:
            # We are purposely using a naive datetime here to work with the
            # rest of the codebase. We can be confident that clients are only
            # sending UTC dates.
            retention_days = enforce_retention(
                message["retention_days"],
                datetime.utcfromtimestamp(message["start_timestamp_ms"] / 1000),
            )
        except EventTooOld:
            return None

        return message, retention_days

    def _process_span_event(
        self,
        processed: MutableMapping[str, Any],
        span_event: SpanEvent,
    ) -> None:
        # process ids
        processed["trace_id"] = str(uuid.UUID(span_event["trace_id"]))
        processed["span_id"] = int(span_event["span_id"], 16)
        processed["segment_id"] = processed["span_id"]
        processed["is_segment"] = span_event["is_segment"]
        parent_span_id: Optional[str] = span_event.get("parent_span_id", None)
        if parent_span_id:
            processed["parent_span_id"] = int(parent_span_id, 16)
        transaction_id: Optional[str] = span_event.get("event_id", None)
        if transaction_id:
            processed["transaction_id"] = str(uuid.UUID(transaction_id))

        # descriptions
        processed["description"] = _unicodify(span_event.get("description", ""))
        processed["group_raw"] = int(span_event.get("group_raw", "0") or "0", 16)

        # timestamps
        processed["start_timestamp"], processed["start_ms"] = self.__extract_timestamp(
            span_event["start_timestamp_ms"] / 1000,
        )
        processed["end_timestamp"], processed["end_ms"] = self.__extract_timestamp(
            (span_event["start_timestamp_ms"] + span_event["duration_ms"]) / 1000,
        )
        processed["duration"] = max(span_event["duration_ms"], 0)
        processed["exclusive_time"] = span_event["exclusive_time_ms"]

    @staticmethod
    def _process_tags(
        processed: MutableMapping[str, Any],
        span_event: SpanEvent,
    ) -> None:
        tags: Mapping[str, Any] = _as_dict_safe(span_event.get("tags", None))
        processed["tags.key"], processed["tags.value"] = extract_extra_tags(tags)
        user = _unicodify(tags.get("sentry:user", ""))
        processed["user"] = user

    @staticmethod
    def _process_measurements(
        processed: MutableMapping[str, Any],
        span_event: SpanEvent,
    ) -> None:
        """
        Extracts measurements from the span_event and writes them into the
        measurements columns.
        """
        processed["measurements.key"] = []
        processed["measurements.value"] = []

        measurements: Any = span_event.get("measurements", {})
        if measurements is not None:
            try:
                (
                    processed["measurements.key"],
                    processed["measurements.value"],
                ) = extract_nested(
                    measurements,
                    lambda value: float(value["value"])
                    if (
                        value is not None
                        and isinstance(value.get("value"), numbers.Number)
                    )
                    else None,
                )
            except Exception:
                logger.error(
                    "Invalid measurements field.",
                    extra={"measurements": measurements},
                    exc_info=True,
                )

    @staticmethod
    def _process_sentry_tags(
        processed: MutableMapping[str, Any],
        span_event: SpanEvent,
    ) -> None:
        """
        TODO: For the top level span belonging to a transaction, we do not know how to fill these
              values yet. For now lets just set them to their default values.
        """
        sentry_tags = span_event["sentry_tags"].copy()
        processed["module"] = sentry_tags.pop("module", "")
        processed["action"] = sentry_tags.pop("action", "")
        processed["domain"] = sentry_tags.pop("domain", "")
        processed["group"] = int(sentry_tags.pop("group", "0") or "0", 16)
        processed["span_kind"] = ""
        processed["platform"] = sentry_tags.pop("system", "")
        processed["segment_name"] = _unicodify(sentry_tags.pop("transaction", ""))

        # TODO: handle status_code so it isn't stored in tags
        status = sentry_tags.pop("status", None)
        if status:
            int_status = SPAN_STATUS_NAME_TO_CODE.get(status, UNKNOWN_SPAN_STATUS)
            processed["status"] = int_status
        else:
            int_status = UNKNOWN_SPAN_STATUS
        processed["span_status"] = int_status

        processed["op"] = sentry_tags.pop("op", "")
        transaction_op = sentry_tags.pop("transaction.op", None)
        if transaction_op:
            processed["transaction_op"] = _unicodify(transaction_op)

        # Remaining tags are dumped into the tags column. In future these will be moved
        # to the sentry_tags column to avoid the danger of conflicting with other tags.
        # This expects that processed_tags has already been called
        leftover_tag_keys, leftover_tag_values = extract_extra_tags(sentry_tags)
        processed["tags.key"].extend(leftover_tag_keys)
        processed["tags.value"].extend(leftover_tag_values)
        processed["sentry_tags.key"].extend(leftover_tag_keys)
        processed["sentry_tags.value"].extend(leftover_tag_values)

    def process_message(
        self,
        message: SpanEvent,
        metadata: KafkaMessageMetadata,
    ) -> Optional[ProcessedMessage]:
        span_event, retention_days = self._structure_and_validate_message(message) or (
            None,
            None,
        )
        if not span_event:
            return None

        processed_rows: MutableSequence[MutableMapping[str, Any]] = []
        processed: MutableMapping[str, Any] = {
            "deleted": 0,
            "retention_days": retention_days,
            "partition": metadata.partition,
            "offset": metadata.offset,
        }

        processed["project_id"] = span_event["project_id"]

        try:
            # The following helper functions should be able to be applied in any order.
            # At time of writing, there are no reads of the values in the `processed`
            # dictionary to inform values in other functions.
            # Ideally we keep continue that rule
            self._process_span_event(processed, span_event)
            self._process_tags(processed, span_event)
            self._process_measurements(processed, span_event)
            self._process_sentry_tags(processed, span_event)
            processed_rows.append(processed)

        except Exception as e:
            metrics.increment("message_processing_error")
            log_bad_span_pct = (
                state.get_float_config("log_bad_span_message_percentage", default=0.0)
                or 0.0
            )
            if random.random() < log_bad_span_pct:
                # key fields in extra_bag are prefixed with "spans_" to avoid conflicts with
                # other fields in LogRecords
                extra_bag = {"spans_" + str(k): v for k, v in message.items()}
                logger.warning(
                    "Failed to process span message", extra=extra_bag, exc_info=e
                )
            return None

        return InsertBatch(rows=processed_rows, origin_timestamp=None)
