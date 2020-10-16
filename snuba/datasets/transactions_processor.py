import logging
import numbers
import uuid
from datetime import datetime
from typing import Any, MutableMapping, Optional

from sentry_relay.consts import SPAN_STATUS_NAME_TO_CODE

from snuba import environment
from snuba.datasets.events_format import (
    enforce_retention,
    extract_base,
    extract_extra_contexts,
    extract_extra_tags,
    extract_http,
    extract_nested,
    extract_user,
)
from snuba.processor import (
    InsertBatch,
    MessageProcessor,
    ProcessedMessage,
    _as_dict_safe,
    _ensure_valid_date,
    _ensure_valid_ip,
    _unicodify,
)
from snuba.utils.metrics.wrapper import MetricsWrapper

logger = logging.getLogger(__name__)

metrics = MetricsWrapper(environment.metrics, "transactions.processor")


UNKNOWN_SPAN_STATUS = 2


class TransactionsMessageProcessor(MessageProcessor):
    PROMOTED_TAGS = {
        "environment",
        "sentry:release",
        "sentry:user",
        "sentry:dist",
    }

    def __extract_timestamp(self, field):
        # We are purposely using a naive datetime here to work with the rest of the codebase.
        # We can be confident that clients are only sending UTC dates.
        timestamp = _ensure_valid_date(datetime.utcfromtimestamp(field))
        if timestamp is None:
            timestamp = datetime.utcnow()
        milliseconds = int(timestamp.microsecond / 1000)
        return (timestamp, milliseconds)

    def process_message(self, message, metadata) -> Optional[ProcessedMessage]:
        processed = {"deleted": 0}
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
        extract_base(processed, event)
        # We are purposely using a naive datetime here to work with the rest of the codebase.
        # We can be confident that clients are only sending UTC dates.
        processed["retention_days"] = enforce_retention(
            event, datetime.utcfromtimestamp(data["timestamp"]),
        )
        if not data.get("contexts", {}).get("trace"):
            return None

        transaction_ctx = data["contexts"]["trace"]
        trace_id = transaction_ctx["trace_id"]
        try:
            processed["event_id"] = str(uuid.UUID(processed["event_id"]))
            processed["trace_id"] = str(uuid.UUID(trace_id))
            processed["span_id"] = int(transaction_ctx["span_id"], 16)
            processed["transaction_op"] = _unicodify(transaction_ctx.get("op") or "")
            processed["transaction_name"] = _unicodify(data.get("transaction") or "")
            processed["start_ts"], processed["start_ms"] = self.__extract_timestamp(
                data["start_timestamp"],
            )

            status = transaction_ctx.get("status", None)
            if status:
                int_status = SPAN_STATUS_NAME_TO_CODE.get(status, UNKNOWN_SPAN_STATUS)
            else:
                int_status = UNKNOWN_SPAN_STATUS

            processed["transaction_status"] = int_status

            if data["timestamp"] - data["start_timestamp"] < 0:
                # Seems we have some negative durations in the DB
                metrics.increment("negative_duration")
        except Exception:
            # all these fields are required but we saw some events go through here
            # in the past.  For now bail.
            return
        processed["finish_ts"], processed["finish_ms"] = self.__extract_timestamp(
            data["timestamp"],
        )

        duration_secs = (processed["finish_ts"] - processed["start_ts"]).total_seconds()
        processed["duration"] = max(int(duration_secs * 1000), 0)

        processed["platform"] = _unicodify(event["platform"])

        tags = _as_dict_safe(data.get("tags", None))
        processed["tags.key"], processed["tags.value"] = extract_extra_tags(tags)

        promoted_tags = {col: tags[col] for col in self.PROMOTED_TAGS if col in tags}
        processed["release"] = promoted_tags.get(
            "sentry:release", event.get("release"),
        )
        processed["environment"] = promoted_tags.get("environment")

        contexts = _as_dict_safe(data.get("contexts", None))

        user_dict = data.get("user", data.get("sentry.interfaces.User", None)) or {}
        geo = user_dict.get("geo", None) or {}
        if "geo" not in contexts and isinstance(geo, dict):
            contexts["geo"] = geo

        measurements = data.get("measurements")
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
        request = data.get("request", data.get("sentry.interfaces.Http", None)) or {}
        http_data: MutableMapping[str, Any] = {}
        extract_http(http_data, request)
        processed["http_method"] = http_data["http_method"]
        processed["http_referer"] = http_data["http_referer"]

        processed["contexts.key"], processed["contexts.value"] = extract_extra_contexts(
            contexts
        )

        processed["dist"] = _unicodify(
            promoted_tags.get("sentry:dist", data.get("dist")),
        )

        user_data = {}
        extract_user(user_data, user_dict)
        processed["user"] = promoted_tags.get("sentry:user", "")
        processed["user_name"] = user_data["username"]
        processed["user_id"] = user_data["user_id"]
        processed["user_email"] = user_data["email"]
        ip_address = _ensure_valid_ip(user_data["ip_address"])

        if ip_address:
            if ip_address.version == 4:
                processed["ip_address_v4"] = str(ip_address)
            elif ip_address.version == 6:
                processed["ip_address_v6"] = str(ip_address)

        processed["partition"] = metadata.partition
        processed["offset"] = metadata.offset

        sdk = data.get("sdk", None) or {}
        processed["sdk_name"] = _unicodify(sdk.get("name") or "")
        processed["sdk_version"] = _unicodify(sdk.get("version") or "")

        if processed["sdk_name"] == "":
            metrics.increment("missing_sdk_name")
        if processed["sdk_version"] == "":
            metrics.increment("missing_sdk_version")

        return InsertBatch([processed])
