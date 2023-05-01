import copy
import logging
import numbers
import random
import uuid
from datetime import datetime
from typing import Any, Dict, Mapping, MutableMapping, MutableSequence, Optional, Tuple

import sqlparse
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
    return False


def parse_query(query: Any, is_savepoint: bool) -> str:
    """
    TODO: This is a temporary solution to extract some useful data from spans until we have a proper
          mechanism of upstream sending us the data we need.
    """
    result = ""
    for token in query:
        if isinstance(token, sqlparse.sql.Where):
            result += parse_query(token, is_savepoint)
        elif isinstance(token, sqlparse.sql.Parenthesis):
            if token.value == "(%s)":
                result += "(...)"
                continue
            result += parse_query(token, is_savepoint)
        elif isinstance(token, sqlparse.sql.Comparison):
            result += parse_query(token, is_savepoint)
        elif isinstance(token, sqlparse.sql.IdentifierList) and "%s" in token.value:
            result += "..."
        elif (
            isinstance(token, sqlparse.sql.Identifier)
            and is_savepoint
            and token.value.upper() != "RELEASE"
        ):
            result += "{savepoint identifier}"
        else:
            result += token.value
    return result


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

    def _process_child_span_module_details(
        self,
        processed_span: MutableMapping[str, Any],
        span_dict: SpanDict,
    ) -> None:
        """
        TODO: This is a bit of a hack, but we need to extract some details from spans temporarily
              until we get proper data from upstream. This code should be removed once we have
              the proper data flowing through the pipeline.
        """
        op = span_dict.get("op", "")
        description = span_dict.get("description", "")

        do_application_logic = state.get_config(
            "spans_process_application_logic", False
        )
        if do_application_logic:
            # Op specific processing
            if op != "db.redis" and op.startswith("db"):
                parse_state = None
                table = ""

                # This is wrong for so many reasons lol, but just something simple to pull some values out
                raw_parsed = sqlparse.parse(description)[0]
                processed_span["description"] = parse_query(
                    raw_parsed, "SAVEPOINT" in description.upper()
                )
                parsed = sqlparse.parse(processed_span["description"])[0]
                operation = None
                processed_span["op"] = parsed.tokens[0].value
                for token in parsed.tokens:
                    if operation is None and token.is_keyword:
                        operation = token.value
                        if token.value == "FROM":
                            operation = "SELECT"

                    if isinstance(token, sqlparse.sql.Comment) or token.is_whitespace:
                        continue
                    elif token.is_keyword:
                        if token.value.lower() == "select":
                            parse_state = "select"
                        elif token.value.lower() == "into":
                            parse_state = "insert"
                        elif token.value.lower() == "delete from":
                            parse_state = "delete"
                        elif token.value.lower() == "update":
                            parse_state = "update"
                        elif token.value.lower() == "from":
                            parse_state = "from"
                        elif token.value.lower() == "order by":
                            parse_state = "order"
                        elif token.value.lower() == "limit":
                            parse_state = "limit"
                        else:
                            parse_state = None
                    elif isinstance(token, sqlparse.sql.Where):
                        condition = ""
                        for t in token.tokens:
                            if isinstance(t, sqlparse.sql.Comment) or t.is_keyword:
                                continue

                            condition += t.value

                        processed_span["tags.key"].append("where")
                        processed_span["tags.value"].append(condition)
                    else:
                        if parse_state in {"from", "update", "delete"}:
                            table = token.value
                        elif parse_state == "insert":
                            table = token.value
                            parse_state = None
                        elif parse_state == "select":
                            processed_span["tags.key"].append("columns")
                            processed_span["tags.value"].append(token.value)
                        elif parse_state == "limit":
                            processed_span["tags.key"].append("limit")
                            processed_span["tags.value"].append(token.value)
                        elif parse_state == "order":
                            processed_span["tags.key"].append("order")
                            processed_span["tags.value"].append(token.value)
                if isinstance(table, str):
                    if "select" in table.lower():
                        table = "join"
                    domain = table.replace('"', "").strip()
                    processed_span["domain"] = domain
                processed_span["op"] = operation.upper() if operation else operation
                processed_span["module"] = "db"
                processed_span["platform"] = "postgres"  # Lying for now
                processed_span["status"] = 0
            elif op == "db.redis":
                processed_span["module"] = "cache"
                parsed = sqlparse.parse(description)[0]
                operation = None
                key = None
                for token in parsed.tokens:
                    if isinstance(token, sqlparse.sql.Comment) or token.is_whitespace:
                        continue
                    elif key is None and ":" in token.value:
                        key = token.value.replace("'", "")
                processed_span["op"] = "unknown"
                processed_span["domain"] = key.split(":")[0] if key else ""
                processed_span["platform"] = "redis"
                processed_span["status"] = 0
            elif op == "http.client":
                processed_span["module"] = "http"
                span_dict_data = span_dict.get("data", {})
                processed_span["op"] = span_dict_data.get("method", "")
                processed_span["status"] = int(span_dict_data.get("status_code", 0))
                url = str(span_dict_data.get("url", ""))
                processed_span["description"] = url
                if url:
                    split_url = url.split("/")
                    processed_span["domain"] = (
                        split_url[2] if len(split_url) > 2 else ""
                    )
            else:
                processed_span["module"] = "unknown"
        else:
            processed_span["op"] = op
            processed_span["description"] = description
            processed_span["module"] = "unknown"
            processed_span["status"] = 0
            processed_span["platform"] = ""
            processed_span["domain"] = ""

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
        tags: Mapping[str, Any] = _as_dict_safe(span_dict.get("data", None))
        span_tag_keys, span_tag_values = extract_extra_tags(tags)
        processed_span["tags.key"].extend(span_tag_keys)
        processed_span["tags.value"].extend(span_tag_values)

        self._process_child_span_module_details(processed_span, span_dict)

        # The processing is of modules does not guarantee that all clickhouse columns would be
        # setup. Just to be safe, lets setup all required clickhouse columns (which are not
        # nullable) with their default values if they have not been setup yet.
        processed_span["span_kind"] = processed_span.get("span_kind", "")
        processed_span["span_status"] = processed_span.get("span_status", 0)
        processed_span["measurements.key"] = processed_span.get("measurements.key", [])
        processed_span["measurements.value"] = processed_span.get(
            "measurements.value", []
        )
        processed_span["module"] = processed_span.get("module", "")

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
