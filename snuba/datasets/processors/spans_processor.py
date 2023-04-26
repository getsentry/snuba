import copy
import logging
import numbers
import uuid
from datetime import datetime
from typing import Any, Dict, Mapping, MutableMapping, MutableSequence, Optional, Tuple

from sentry_relay.consts import SPAN_STATUS_NAME_TO_CODE

from snuba import environment
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
from snuba.state import get_config
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

    # Tags which need to be pushed down to each individual span
    PUSH_DOWN_TAGS = {
        "environment",
        "release",
    }

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

        processed["project_id"] = common_span_fields["project_id"] = event_dict[
            "project_id"
        ]
        processed["transaction_id"] = str(uuid.UUID(event_dict["event_id"]))

        transaction_ctx = event_dict["data"]["contexts"]["trace"]
        trace_id = transaction_ctx["trace_id"]
        processed["trace_id"] = str(uuid.UUID(trace_id))
        processed["span_id"] = int(transaction_ctx["span_id"], 16)
        parent_span_id = transaction_ctx.get("parent_span_id", 0)
        processed["parent_span_id"] = int(parent_span_id, 16) if parent_span_id else 0
        processed["segment_id"] = common_span_fields["segment_id"] = processed[
            "span_id"
        ]
        processed["is_segment"] = 1
        processed["description"] = _unicodify(event_dict.get("description", ""))
        processed["op"] = _unicodify(transaction_ctx.get("op", ""))
        processed["group"] = int(transaction_ctx.get("hash", 0), 16)
        processed["segment_name"] = common_span_fields["segment_name"] = _unicodify(
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

        processed["platform"] = _unicodify(event_dict["platform"])

    @staticmethod
    def _process_tags(
        processed: MutableMapping[str, Any],
        event_dict: EventDict,
        common_span_fields: CommonSpanDict,
    ) -> None:
        tags: Mapping[str, Any] = _as_dict_safe(event_dict["data"].get("tags", None))
        processed["tags.key"], processed["tags.value"] = extract_extra_tags(tags)

        span_environment = _unicodify(tags.get("environment", ""))
        release = _unicodify(tags.get("sentry:release", event_dict.get("release", "")))
        user = _unicodify(tags.get("sentry:user", ""))
        processed["user"] = user

        common_span_fields["tags.key"] = ["environment", "release", "user"]
        common_span_fields["tags.value"] = [span_environment, release, user]

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
    def _process_module(
        processed: MutableMapping[str, Any],
        event_dict: EventDict,
    ) -> None:
        """
        TODO: This needs to be filled in once data from relay starts flowing in according to the
        desired schema. For now we will set it to empty strings.
        """
        processed["module"] = ""
        processed["action"] = ""
        processed["domain"] = ""
        processed["status"] = 0
        processed["span_kind"] = ""

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
        processed_span["transaction_id"] = None
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
        tags: Mapping[str, Any] = _as_dict_safe(span_dict.get("data", None))
        span_tag_keys, span_tag_values = extract_extra_tags(tags)
        processed_span["tags.key"].extend(span_tag_keys)
        processed_span["tags.value"].extend(span_tag_values)

        # TODO: The current data model does not allow us to set these values but they are needed
        #  in order to write data to the spans table. Lets set them to the default values for now.
        processed_span["status"] = 0
        processed_span["span_kind"] = ""
        processed_span["span_status"] = 0
        processed_span["platform"] = ""
        processed_span["user"] = ""
        processed_span["measurements.key"] = []
        processed_span["measurements.value"] = []
        processed_span["module"] = ""
        processed_span["action"] = ""
        processed_span["domain"] = ""

        return processed_span

    def _process_spans(
        self, event_dict: EventDict, common_span_fields: CommonSpanDict
    ) -> MutableSequence[MutableMapping[str, Any]]:
        data = event_dict["data"]

        try:
            max_spans_per_transaction = get_config("max_spans_per_transaction", 2000)
            assert isinstance(max_spans_per_transaction, (int, float))
        except Exception:
            metrics.increment("bad_config.max_spans_per_transaction")
            max_spans_per_transaction = 2000

        num_processed = 0
        processed_spans = []

        for span in data.get("spans", []):
            # The number of spans should not exceed 1000 as enforced by SDKs.
            # As a safety precaution, enforce a hard limit on the number of
            # spans we actually store .
            if num_processed >= max_spans_per_transaction:
                metrics.increment("too_many_spans")
                break

            processed_span = self._process_span(span, common_span_fields)

            if processed_span is not None:
                num_processed += 1
                processed_spans.append(processed_span)

        return processed_spans

    def process_message(
        self,
        message: Tuple[int, str, Mapping[Any, Any]],
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
        # The following helper functions should be able to be applied in any order.
        # At time of writing, there are no reads of the values in the `processed`
        # dictionary to inform values in other functions.
        # Ideally we keep continue that rule
        self._process_base_event_values(processed, event_dict, common_span_fields)
        self._process_tags(processed, event_dict, common_span_fields)
        self._process_measurements(processed, event_dict)
        self._process_module(processed, event_dict)
        processed_rows.append(processed)
        processed_spans = self._process_spans(event_dict, common_span_fields)
        processed_rows.extend(processed_spans)

        print(processed_rows)
        return InsertBatch(rows=processed_rows, origin_timestamp=None)
