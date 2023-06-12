import copy
import logging
import numbers
import random
import uuid
from datetime import datetime
from typing import Any, Dict, Mapping, MutableMapping, MutableSequence, Optional, Tuple

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
    project_allowlist = state.get_config("spans_project_allowlist", None)
    if project_allowlist:
        # The expected format is [project,project,...]
        project_allowlist = project_allowlist[1:-1]
        if project_allowlist:
            rolled_out_projects = [int(p.strip()) for p in project_allowlist.split(",")]
            if project_id in rolled_out_projects:
                return True
    return True


def clean_span_tags(tags: Mapping[str, Any]) -> MutableMapping[str, Any]:
    """
    A lot of metadata regarding spans is sent in spans.data. We do not want to store everything
    as tags in clickhouse. This method allows only a few pre defined keys to be stored as tags.
    """
    allowed_keys = {"environment", "release", "user"}
    return {k: v for k, v in tags.items() if k in allowed_keys}


class SpansMessageProcessor(DatasetMessageProcessor):
    """
    Message processor for writing spans data to the spans table.
    The implementation has taken inspiration from the transactions processor.
    The initial version of this processor is able to read existing data
    from the transactions topic and de-normalize it into the spans table.
    """

    def __extract_timestamp(self, field: int) -> Tuple[datetime, int]:
        # We are purposely using a naive datetime here to work with the rest of the codebase.
        # We can be confident that clients are only sending UTC dates.
        timestamp = _ensure_valid_date(datetime.utcfromtimestamp(field))
        if timestamp is None:
            timestamp = datetime.utcnow()
        milliseconds = int(timestamp.microsecond / 1000)
        return timestamp, milliseconds

    @staticmethod
    def _structure_and_validate_message(
        message: Tuple[int, str, Dict[str, Any]]
    ) -> Optional[Tuple[EventDict, RetentionDays]]:
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

        if not data.get("contexts", {}).get("trace"):
            return None
        try:
            # We are purposely using a naive datetime here to work with the
            # rest of the codebase. We can be confident that clients are only
            # sending UTC dates.
            retention_days = enforce_retention(
                event.get("retention_days"),
                datetime.utcfromtimestamp(data["timestamp"]),
            )
        except EventTooOld:
            return None

        return event, retention_days

    def _process_base_event_values(
        self,
        processed: MutableMapping[str, Any],
        event_dict: EventDict,
        common_span_fields: CommonSpanDict,
    ) -> None:
        processed["transaction_id"] = common_span_fields["transaction_id"] = str(
            uuid.UUID(event_dict["event_id"])
        )

        transaction_ctx = event_dict["data"]["contexts"]["trace"]
        trace_id = transaction_ctx["trace_id"]
        processed["trace_id"] = str(uuid.UUID(trace_id))
        processed["span_id"] = int(transaction_ctx["span_id"], 16)
        processed["segment_id"] = common_span_fields["segment_id"] = processed[
            "span_id"
        ]
        processed["is_segment"] = 1
        parent_span_id = transaction_ctx.get("parent_span_id", 0)
        processed["parent_span_id"] = int(parent_span_id, 16) if parent_span_id else 0

        processed["description"] = _unicodify(event_dict.get("description", ""))
        processed["op"] = common_span_fields["transaction_op"] = _unicodify(
            transaction_ctx.get("op", "")
        )
        processed["transaction_op"] = processed["op"]

        span_hash = transaction_ctx.get("hash", None)
        processed["group"] = 0 if not span_hash else int(span_hash, 16)
        processed["segment_name"] = _unicodify(
            event_dict["data"].get("transaction") or ""
        )

        processed["start_timestamp"], _ = self.__extract_timestamp(
            event_dict["data"]["start_timestamp"],
        )
        if event_dict["data"]["timestamp"] - event_dict["data"]["start_timestamp"] < 0:
            # Seems we have some negative durations in the DB
            metrics.increment("negative_duration")

        processed["end_timestamp"], _ = self.__extract_timestamp(
            event_dict["data"]["timestamp"],
        )
        duration_secs = (
            processed["end_timestamp"] - processed["start_timestamp"]
        ).total_seconds()
        processed["duration"] = max(int(duration_secs * 1000), 0)
        processed["exclusive_time"] = transaction_ctx.get("exclusive_time", 0)

        status = transaction_ctx.get("status", None)
        if status:
            int_status = SPAN_STATUS_NAME_TO_CODE.get(status, UNKNOWN_SPAN_STATUS)
        else:
            int_status = UNKNOWN_SPAN_STATUS
        processed["span_status"] = int_status

    @staticmethod
    def _process_tags(
        processed: MutableMapping[str, Any],
        event_dict: EventDict,
        common_span_fields: CommonSpanDict,
    ) -> None:
        tags: Mapping[str, Any] = _as_dict_safe(event_dict["data"].get("tags", None))
        processed["tags.key"], processed["tags.value"] = extract_extra_tags(tags)

        release = _unicodify(tags.get("sentry:release", event_dict.get("release", "")))
        user = _unicodify(tags.get("sentry:user", ""))
        processed["user"] = user

        common_span_fields["tags.key"] = ["release", "user"]
        common_span_fields["tags.value"] = [release, user]

    @staticmethod
    def _process_measurements(
        processed: MutableMapping[str, Any],
        event_dict: EventDict,
    ) -> None:
        """
        Extracts measurements from the event_dict and writes them into the
        measurements columns.
        """
        measurements = event_dict["data"].get("measurements")
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
    def _process_module_details(
        processed: MutableMapping[str, Any],
        event_dict: EventDict,
    ) -> None:
        """
        TODO: For the top level span belonging to a transaction, we do not know how to fill these
              values yet. For now lets just set them to their default values.
        """
        processed["module"] = ""
        processed["action"] = ""
        processed["domain"] = ""
        processed["status"] = 0
        processed["span_kind"] = ""
        processed["platform"] = ""

    def _process_child_span_module_details(
        self,
        processed_span: MutableMapping[str, Any],
        span_dict: SpanDict,
    ) -> None:
        span_data = span_dict.get("data", None)
        op = span_dict.get("op", "")
        description = span_dict.get("description", "")
        if span_data is None:
            # In case we have a span without data, we can't do anything about few of the fields.
            processed_span["op"] = op
            processed_span["description"] = description
            processed_span["module"] = "unknown"
            processed_span["span_status"] = 0
            processed_span["domain"] = ""
            processed_span["platform"] = ""
            processed_span["action"] = ""
            processed_span["status"] = 0
            return

        processed_span["op"] = _unicodify(span_data.get("span.op", op))
        processed_span["description"] = _unicodify(
            span_data.get("span.description", description)
        )
        processed_span["module"] = _unicodify(span_data.get("span.module", "unknown"))
        span_status = span_data.get("span.status", None)
        if span_status:
            processed_span["span_status"] = SPAN_STATUS_NAME_TO_CODE.get(
                span_status, UNKNOWN_SPAN_STATUS
            )
        else:
            processed_span["span_status"] = UNKNOWN_SPAN_STATUS
        processed_span["domain"] = _unicodify(span_data.get("span.domain", ""))
        processed_span["platform"] = _unicodify(span_data.get("span.system", ""))
        processed_span["action"] = _unicodify(span_data.get("span.action", ""))
        processed_span["status"] = span_data.get("span.status_code", 0)

    def _process_span(
        self, span_dict: SpanDict, common_span_fields: CommonSpanDict
    ) -> MutableMapping[str, Any]:
        """
        Use the individual span data from a transaction to create individual span rows.
        This is needed for the first version of the implementation until we can start
        getting span data from the spans topic.
        """
        processed_span: MutableMapping[str, Any] = {}

        # Copy the common span fields into the processed span since common span field
        # holds common tags which are mutable and we do not want them to be modified
        # while processing different spans.
        processed_span.update(copy.deepcopy(common_span_fields))
        processed_span["trace_id"] = str(uuid.UUID(span_dict["trace_id"]))
        processed_span["span_id"] = int(span_dict["span_id"], 16)
        processed_span["parent_span_id"] = int(span_dict.get("parent_span_id", 0), 16)
        processed_span["is_segment"] = 0
        processed_span["op"] = _unicodify(span_dict.get("op", ""))
        processed_span["group"] = int(span_dict.get("hash", 0), 16)
        processed_span["exclusive_time"] = span_dict.get("exclusive_time", 0)
        processed_span["description"] = _unicodify(span_dict.get("description", ""))

        start_timestamp = span_dict["start_timestamp"]
        end_timestamp = span_dict["timestamp"]
        processed_span["start_timestamp"], _ = self.__extract_timestamp(start_timestamp)
        if end_timestamp - start_timestamp < 0:
            # Seems we have some negative durations in the DB
            metrics.increment("negative_duration")

        processed_span["end_timestamp"], _ = self.__extract_timestamp(end_timestamp)
        duration_secs = (
            processed_span["end_timestamp"] - processed_span["start_timestamp"]
        ).total_seconds()
        processed_span["duration"] = max(int(duration_secs * 1000), 0)
        processed_span["exclusive_time"] = span_dict.get("exclusive_time", 0)

        span_data: Mapping[str, Any] = _as_dict_safe(span_dict.get("data", None))
        if span_data:
            processed_span["segment_name"] = _unicodify(
                span_data.get("transaction", "")
            )
            cleaned_tags = clean_span_tags(span_data)
            span_tag_keys, span_tag_values = extract_extra_tags(cleaned_tags)
            processed_span["tags.key"].extend(span_tag_keys)
            processed_span["tags.value"].extend(span_tag_values)

        self._process_child_span_module_details(processed_span, span_dict)

        # The processing is of modules does not guarantee that all clickhouse columns would be
        # setup. Just to be safe, lets setup all required clickhouse columns (which are not
        # nullable) with their default values if they have not been setup yet.
        processed_span["span_kind"] = processed_span.get("span_kind", "")
        processed_span["measurements.key"] = processed_span.get("measurements.key", [])
        processed_span["measurements.value"] = processed_span.get(
            "measurements.value", []
        )

        return processed_span

    def _process_spans(
        self, event_dict: EventDict, common_span_fields: CommonSpanDict
    ) -> MutableSequence[MutableMapping[str, Any]]:
        data = event_dict["data"]
        processed_spans: MutableSequence[MutableMapping[str, Any]] = []
        for span in data.get("spans", []):
            processed_span = self._process_span(span, common_span_fields)
            if processed_span is not None:
                processed_spans.append(processed_span)

        return processed_spans

    def process_message(
        self,
        message: Tuple[int, str, Dict[Any, Any]],
        metadata: KafkaMessageMetadata,
    ) -> Optional[ProcessedMessage]:
        event_dict, retention_days = self._structure_and_validate_message(message) or (
            None,
            None,
        )
        if not event_dict:
            return None

        processed_rows: MutableSequence[MutableMapping[str, Any]] = []
        common_span_fields: MutableMapping[str, Any] = {
            "deleted": 0,
            "retention_days": retention_days,
            "partition": metadata.partition,
            "offset": metadata.offset,
        }

        processed: MutableMapping[str, Any] = {}
        processed.update(common_span_fields)

        processed["project_id"] = common_span_fields["project_id"] = event_dict[
            "project_id"
        ]

        # Reject events from projects that are not in the allowlist
        if not is_project_in_allowlist(processed["project_id"]):
            return None

        try:
            # The following helper functions should be able to be applied in any order.
            # At time of writing, there are no reads of the values in the `processed`
            # dictionary to inform values in other functions.
            # Ideally we keep continue that rule
            self._process_base_event_values(processed, event_dict, common_span_fields)
            self._process_tags(processed, event_dict, common_span_fields)
            self._process_measurements(processed, event_dict)
            self._process_module_details(processed, event_dict)
            processed_rows.append(processed)
            processed_spans = self._process_spans(event_dict, common_span_fields)
            processed_rows.extend(processed_spans)

        except Exception as e:
            metrics.increment("message_processing_error")
            log_bad_span_pct = state.get_config(
                "log_bad_span_message_percentage", default=0.0
            )
            if random.random() < float(log_bad_span_pct if log_bad_span_pct else 0.0):
                logger.warning(
                    "Failed to process span message", extra=message[2], exc_info=e
                )
            return None

        return InsertBatch(rows=processed_rows, origin_timestamp=None)
