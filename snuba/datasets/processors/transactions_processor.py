import copy
import logging
import numbers
import uuid
from datetime import datetime
from typing import Any, Dict, Mapping, MutableMapping, Optional, Tuple

from sentry_relay.consts import SPAN_STATUS_NAME_TO_CODE

from snuba import environment, settings
from snuba.consumers.types import KafkaMessageMetadata
from snuba.datasets.events_format import (
    EventTooOld,
    enforce_retention,
    extract_base,
    extract_extra_contexts,
    extract_extra_tags,
    extract_http,
    extract_nested,
    extract_user,
)
from snuba.datasets.processors import DatasetMessageProcessor
from snuba.processor import (
    InsertBatch,
    ProcessedMessage,
    _as_dict_safe,
    _collapse_uint32,
    _ensure_valid_date,
    _ensure_valid_ip,
    _unicodify,
)
from snuba.state import get_config
from snuba.utils.metrics.wrapper import MetricsWrapper

logger = logging.getLogger(__name__)

metrics = MetricsWrapper(environment.metrics, "transactions.processor")


UNKNOWN_SPAN_STATUS = 2
GROUP_IDS_LIMIT = 10

EventDict = Dict[str, Any]
SpanDict = Dict[str, Any]
RetentionDays = int


class TransactionsMessageProcessor(DatasetMessageProcessor):
    PROMOTED_TAGS = {
        "environment",
        "sentry:release",
        "sentry:user",
        "sentry:dist",
        "replayId",
    }

    def __extract_timestamp(self, field: int) -> Tuple[datetime, int]:
        # We are purposely using a naive datetime here to work with the rest of the codebase.
        # We can be confident that clients are only sending UTC dates.
        timestamp = _ensure_valid_date(datetime.utcfromtimestamp(field))
        if timestamp is None:
            timestamp = datetime.utcnow()
        milliseconds = int(timestamp.microsecond / 1000)
        return (timestamp, milliseconds)

    def _structure_and_validate_message(
        self, message: Tuple[int, str, Dict[str, Any]]
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
        self, processed: MutableMapping[str, Any], event_dict: EventDict
    ) -> MutableMapping[str, Any]:

        extract_base(processed, event_dict)

        transaction_ctx = event_dict["data"]["contexts"]["trace"]
        trace_id = transaction_ctx["trace_id"]
        processed["event_id"] = str(uuid.UUID(processed["event_id"]))
        processed["trace_id"] = str(uuid.UUID(trace_id))
        processed["span_id"] = int(transaction_ctx["span_id"], 16)
        processed["transaction_op"] = _unicodify(transaction_ctx.get("op") or "")
        processed["transaction_name"] = _unicodify(
            event_dict["data"].get("transaction") or ""
        )
        processed["transaction_source"] = _unicodify(
            (event_dict["data"].get("transaction_info") or {}).get("source") or ""
        )
        processed["start_ts"], processed["start_ms"] = self.__extract_timestamp(
            event_dict["data"]["start_timestamp"],
        )
        status = transaction_ctx.get("status", None)
        if status:
            int_status = SPAN_STATUS_NAME_TO_CODE.get(status, UNKNOWN_SPAN_STATUS)
        else:
            int_status = UNKNOWN_SPAN_STATUS

        processed["transaction_status"] = int_status
        if event_dict["data"]["timestamp"] - event_dict["data"]["start_timestamp"] < 0:
            # Seems we have some negative durations in the DB
            metrics.increment("negative_duration")

        processed["finish_ts"], processed["finish_ms"] = self.__extract_timestamp(
            event_dict["data"]["timestamp"],
        )

        duration_secs = (processed["finish_ts"] - processed["start_ts"]).total_seconds()
        processed["duration"] = max(int(duration_secs * 1000), 0)

        processed["platform"] = _unicodify(event_dict["platform"])

        group_ids = event_dict.get("group_ids") or []
        if len(group_ids) > GROUP_IDS_LIMIT:
            metrics.increment("group_ids_exceeded_limit")

        processed["group_ids"] = group_ids[:GROUP_IDS_LIMIT]

        return processed

    def _process_tags(
        self,
        processed: MutableMapping[str, Any],
        event_dict: EventDict,
    ) -> None:

        tags: Mapping[str, Any] = _as_dict_safe(event_dict["data"].get("tags", None))
        processed["tags.key"], processed["tags.value"] = extract_extra_tags(tags)
        promoted_tags = {col: tags[col] for col in self.PROMOTED_TAGS if col in tags}
        processed["release"] = promoted_tags.get(
            "sentry:release",
            event_dict.get("release"),
        )
        processed["environment"] = promoted_tags.get("environment")
        processed["user"] = promoted_tags.get("sentry:user", "")

        replay_id = promoted_tags.get("replayId")
        if replay_id:
            try:
                processed["replay_id"] = str(uuid.UUID(replay_id))
            except ValueError:
                # replay_id as a tag is not guarenteed to be UUID (user could set value in theory)
                # so simply continue if not UUID.
                pass

        processed["dist"] = _unicodify(
            promoted_tags.get("sentry:dist", event_dict["data"].get("dist")),
        )

    def _process_measurements(
        self,
        processed: MutableMapping[str, Any],
        event_dict: EventDict,
    ) -> None:
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
                # Not failing the event in this case just yet, because we are still
                # developing this feature.
                logger.error(
                    "Invalid measurements field.",
                    extra={"measurements": measurements},
                    exc_info=True,
                )

    def _process_breakdown(
        self,
        processed: MutableMapping[str, Any],
        event_dict: EventDict,
    ) -> None:
        breakdowns = event_dict["data"].get("breakdowns")
        if breakdowns is not None:
            span_op_breakdowns = breakdowns.get("span_ops")
            if span_op_breakdowns is not None:
                try:
                    (
                        processed["span_op_breakdowns.key"],
                        processed["span_op_breakdowns.value"],
                    ) = extract_nested(
                        span_op_breakdowns,
                        lambda value: float(value["value"])
                        if (
                            value is not None
                            and isinstance(value.get("value"), numbers.Number)
                        )
                        else None,
                    )
                except Exception:
                    # Not failing the event in this case just yet, because we are still
                    # developing this feature.
                    logger.error(
                        "Invalid breakdowns.span_ops field.",
                        extra={"span_op_breakdowns": span_op_breakdowns},
                        exc_info=True,
                    )

    def _process_contexts_and_user(
        self,
        processed: MutableMapping[str, Any],
        event_dict: EventDict,
    ) -> None:
        contexts: MutableMapping[str, Any] = _as_dict_safe(
            event_dict["data"].get("contexts", None)
        )
        user_dict = (
            event_dict["data"].get(
                "user", event_dict["data"].get("sentry.interfaces.User", None)
            )
            or {}
        )
        geo = user_dict.get("geo", None) or {}

        if "geo" not in contexts and isinstance(geo, dict):
            contexts["geo"] = geo

        skipped_contexts = settings.TRANSACT_SKIP_CONTEXT_STORE.get(
            processed["project_id"], set()
        )
        for context in skipped_contexts:
            if context in contexts:
                del contexts[context]

        app_context = contexts.get("app")
        if app_context is not None:
            app_start_type = app_context.get("start_type")
            if app_start_type is not None:
                processed["app_start_type"] = app_start_type

        profile_context = contexts.get("profile")
        if profile_context is not None:
            profile_id = profile_context.get("profile_id")
            if profile_id is not None:
                processed["profile_id"] = str(uuid.UUID(profile_id))

        replay_context = contexts.get("replay")
        if replay_context is not None:
            replay_id = replay_context.get("replay_id")
            if replay_id is not None:
                replay_id_uuid = uuid.UUID(replay_id)
                processed["replay_id"] = str(replay_id_uuid)

                # remove this after 90 days or we shift query conditions based on timestamp
                if "replayId" not in processed["tags.key"]:
                    processed["tags.key"].append("replayId")
                    # the tag value should not have dashes in it so we use .hex
                    processed["tags.value"].append(replay_id_uuid.hex)

        sanitized_contexts = self._sanitize_contexts(processed, event_dict)
        processed["contexts.key"], processed["contexts.value"] = extract_extra_contexts(
            sanitized_contexts
        )

        user_data: MutableMapping[str, Any] = {}
        extract_user(user_data, user_dict)
        processed["user_name"] = user_data["username"]
        processed["user_id"] = user_data["user_id"]
        processed["user_email"] = user_data["email"]
        ip_address = _ensure_valid_ip(user_data["ip_address"])
        if ip_address:
            if ip_address.version == 4:
                processed["ip_address_v4"] = str(ip_address)
            elif ip_address.version == 6:
                processed["ip_address_v6"] = str(ip_address)

    def _process_request_data(
        self,
        processed: MutableMapping[str, Any],
        event_dict: EventDict,
    ) -> None:
        request = (
            event_dict["data"].get(
                "request", event_dict["data"].get("sentry.interfaces.Http", None)
            )
            or {}
        )
        http_data: MutableMapping[str, Any] = {}
        extract_http(http_data, request)
        processed["http_method"] = http_data["http_method"]
        processed["http_referer"] = http_data["http_referer"]

    def _process_sdk_data(
        self,
        processed: MutableMapping[str, Any],
        event_dict: EventDict,
    ) -> None:
        sdk = event_dict["data"].get("sdk", None) or {}
        processed["sdk_name"] = _unicodify(sdk.get("name") or "")
        processed["sdk_version"] = _unicodify(sdk.get("version") or "")

        if processed["sdk_name"] == "":
            metrics.increment("missing_sdk_name")
        if processed["sdk_version"] == "":
            metrics.increment("missing_sdk_version")

    def _process_span(self, span_dict: SpanDict) -> Optional[Tuple[str, int, float]]:
        op = span_dict.get("op")
        group = span_dict.get("hash")
        exclusive_time = span_dict.get("exclusive_time")

        if op is None or group is None or exclusive_time is None:
            return None

        return op, int(group, 16), exclusive_time

    def _process_spans(
        self,
        processed: MutableMapping[str, Any],
        event_dict: EventDict,
    ) -> None:
        data = event_dict["data"]
        trace_context = data["contexts"]["trace"]

        try:
            max_spans_per_transaction = get_config("max_spans_per_transaction", 2000)
            assert isinstance(max_spans_per_transaction, (int, float))
        except Exception:
            metrics.increment("bad_config.max_spans_per_transaction")
            max_spans_per_transaction = 2000

        num_processed = 0
        processed_spans = []

        processed_root_span = self._process_span(trace_context)
        if processed_root_span is not None:
            num_processed += 1
            processed_spans.append(processed_root_span)

        for span in data.get("spans", []):
            # The number of spans should not exceed 1000 as enforced by SDKs.
            # As a safety precaution, enforce a hard limit on the number of
            # spans we actually store .
            if num_processed >= max_spans_per_transaction:
                metrics.increment("too_many_spans")
                break

            processed_span = self._process_span(span)

            if processed_span is not None:
                num_processed += 1
                processed_spans.append(processed_span)

        processed["spans.op"] = []
        processed["spans.group"] = []
        processed["spans.exclusive_time"] = []
        processed["spans.exclusive_time_32"] = []

        for op, group, exclusive_time in sorted(processed_spans):
            processed["spans.op"].append(op)
            processed["spans.group"].append(group)
            processed["spans.exclusive_time"].append(0)
            processed["spans.exclusive_time_32"].append(exclusive_time)

    def _sanitize_contexts(
        self,
        processed: MutableMapping[str, Any],
        event_dict: EventDict,
    ) -> MutableMapping[str, Any]:
        """
        Contexts can store a lot of data. We don't want to store all the data in
        the database. This api removes elements from contexts which are not required
        to be stored. It does that by creating a deepcopy of the contexts and working
        with the deepcopy so that the contexts in the original message are not
        mutated. It returns the new modified context object which can be used to fill
        in the processed object.
        """
        contexts: MutableMapping[str, Any] = _as_dict_safe(
            event_dict["data"].get("contexts", None)
        )
        if not contexts:
            return {}

        sanitized_context = copy.deepcopy(contexts)
        # We store trace_id and span_id as promoted columns and on the query level
        # we make sure that all queries on contexts[trace.trace_id/span_id] use those promoted
        # columns instead. So we don't need to store them in the contexts array as well
        transaction_ctx = sanitized_context.get("trace", {})
        transaction_ctx.pop("trace_id", None)
        transaction_ctx.pop("span_id", None)

        # The hash and exclusive_time is being stored in the spans columns
        # so there is no need to store it again in the context array.
        transaction_ctx.pop("hash", None)
        transaction_ctx.pop("exclusive_time", None)

        # The app_start_type is promoted as a column on a query level, no need to store it again
        # in the context array.
        app_ctx = sanitized_context.get("app", {})
        if app_ctx is not None:
            app_ctx.pop("start_type", None)

        # The profile_id and replay_id are promoted as columns, so no need to store them
        # again in the context array
        profile_ctx = sanitized_context.get("profile", {})
        profile_ctx.pop("profile_id", None)
        replay_ctx = sanitized_context.get("replay", {})
        replay_ctx.pop("replay_id", None)

        skipped_contexts = settings.TRANSACT_SKIP_CONTEXT_STORE.get(
            processed["project_id"], set()
        )
        for context in skipped_contexts:
            if context in sanitized_context:
                del sanitized_context[context]

        return sanitized_context

    def process_message(
        self, message: Tuple[int, str, Dict[Any, Any]], metadata: KafkaMessageMetadata
    ) -> Optional[ProcessedMessage]:
        event_dict, retention_days = self._structure_and_validate_message(message) or (
            None,
            None,
        )
        if not event_dict:
            return None
        processed: MutableMapping[str, Any] = {
            "deleted": 0,
            "retention_days": retention_days,
        }
        # The following helper functions should be able to be applied in any order.
        # At time of writing, there are no reads of the values in the `processed`
        # dictionary to inform values in other functions.
        # Ideally we keep continue that rule
        self._process_base_event_values(processed, event_dict)
        self._process_tags(processed, event_dict)
        self._process_measurements(processed, event_dict)
        self._process_breakdown(processed, event_dict)
        self._process_spans(processed, event_dict)
        self._process_request_data(processed, event_dict)
        self._process_sdk_data(processed, event_dict)
        processed["partition"] = metadata.partition
        processed["offset"] = metadata.offset

        # the following operation modifies the event_dict and is therefore *not* order-independent
        self._process_contexts_and_user(processed, event_dict)

        try:
            raw_received = _collapse_uint32(int(event_dict["data"]["received"]))
            assert raw_received is not None
            received = datetime.utcfromtimestamp(raw_received)
            return InsertBatch([processed], received)
        except Exception:
            raise KeyError("Missing received timestamp field in transaction")
