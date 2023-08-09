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


def is_project_in_allowlist(project_id: int) -> bool:
    """
    Allow spans to be written to Clickhouse if the project falls into one of the following
    categories in order of priority:
    1. The project falls in the configured sample rate
    2. The project is in the allowlist
    """
    spans_sample_rate = state.get_config("spans_sample_rate", None)
    if spans_sample_rate and project_id % 100 <= spans_sample_rate:
        return True

    project_allowlist = state.get_config("spans_project_allowlist", None)
    if project_allowlist:
        # The expected format is [project,project,...]
        project_allowlist = project_allowlist[1:-1]
        if project_allowlist:
            rolled_out_projects = [int(p.strip()) for p in project_allowlist.split(",")]
            if project_id in rolled_out_projects:
                return True
    return False


def clean_span_tags(tags: Mapping[str, Any]) -> MutableMapping[str, Any]:
    """
    A lot of metadata regarding spans is sent in spans.data. We do not want to store everything
    as tags in clickhouse. This method allows only a few pre defined keys to be stored as tags.
    """
    allowed_keys = {"environment", "release", "user", "transaction.method"}
    return {k: v for k, v in tags.items() if k in allowed_keys}


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
        processed["trace_id"] = str(uuid.UUID(span_event["trace_id"]))
        processed["span_id"] = int(span_event["span_id"], 16)
        processed["segment_id"] = processed["span_id"]
        processed["is_segment"] = span_event["is_segment"]
        parent_span_id: str = span_event.get("parent_span_id", "0")
        processed["parent_span_id"] = int(parent_span_id, 16) if parent_span_id else 0

        processed["description"] = _unicodify(span_event.get("description", ""))
        processed["op"] = _unicodify(span_event["sentry_tags"]["transaction.op"])
        processed["transaction_op"] = processed["op"]

        span_hash_raw = span_event.get("group_raw", None)
        processed["group_raw"] = 0 if not span_hash_raw else int(str(span_hash_raw), 16)

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

        # release = _unicodify(tags.get("sentry:release", span_event.get("release", "")))
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
        sentry_tags = span_event["sentry_tags"]
        processed["module"] = sentry_tags["module"]
        processed["action"] = sentry_tags["action"]
        processed["domain"] = sentry_tags["domain"]
        processed["status"] = sentry_tags["status"]
        group = sentry_tags["group"]
        processed["group"] = int(str(group), 16) if group else 0
        processed["span_kind"] = ""
        processed["platform"] = sentry_tags["system"]
        processed["segment_name"] = _unicodify(sentry_tags.get("transaction") or "")

        status = sentry_tags.get("status", None)
        if status:
            int_status = SPAN_STATUS_NAME_TO_CODE.get(status, UNKNOWN_SPAN_STATUS)
        else:
            int_status = UNKNOWN_SPAN_STATUS
        processed["span_status"] = int_status

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

        # Reject events from projects that are not in the allowlist
        if not is_project_in_allowlist(processed["project_id"]):
            return None

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
