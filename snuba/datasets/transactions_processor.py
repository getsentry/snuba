from datetime import datetime
from typing import Optional

import uuid

from snuba.processor import (
    _as_dict_safe,
    MessageProcessor,
    ProcessorAction,
    ProcessedMessage,
    _ensure_valid_date,
    _ensure_valid_ip,
    _unicodify
)
from snuba.datasets.events_processor import (
    enforce_retention,
    extract_base,
    extract_extra_contexts,
    extract_extra_tags,
    extract_user,
)


class TransactionsMessageProcessor(MessageProcessor):
    PROMOTED_TAGS = {
        "environment",
        "sentry:release",
        "sentry:user",
        "sentry:dist",
    }

    def __extract_timestamp(self, field):
        timestamp = _ensure_valid_date(datetime.fromtimestamp(field))
        if timestamp is None:
            timestamp = datetime.utcnow()
        milliseconds = int(timestamp.microsecond / 1000)
        return (timestamp, milliseconds)

    def process_message(self, message, metadata=None) -> Optional[ProcessedMessage]:
        action_type = ProcessorAction.INSERT
        processed = {'deleted': 0}
        if not (isinstance(message, (list, tuple)) and len(message) >= 2):
            return None
        version = message[0]
        if version not in (0, 1, 2):
            return None
        type_, event = message[1:3]
        if type_ != 'insert':
            return None

        data = event["data"]
        event_type = data.get("type")
        if event_type != "transaction":
            return None
        extract_base(processed, event)
        processed["retention_days"] = enforce_retention(
            event,
            datetime.fromtimestamp(data['timestamp']),
        )
        if not data.get('contexts', {}).get('trace'):
            return None

        transaction_ctx = data["contexts"]["trace"]
        trace_id = transaction_ctx["trace_id"]
        try:
            processed["event_id"] = str(uuid.UUID(processed["event_id"]))
            processed["trace_id"] = str(uuid.UUID(trace_id))
            processed["span_id"] = int(transaction_ctx["span_id"], 16)
            processed["transaction_op"] = _unicodify(transaction_ctx.get("op", ""))
            processed["transaction_name"] = _unicodify(data["transaction"])
            processed["start_ts"], processed["start_ms"] = self.__extract_timestamp(
                data["start_timestamp"],
            )
        except KeyError:
            # all these fields are required but we saw some events go through here
            # in the past.  For now bail.
            return
        processed["finish_ts"], processed["finish_ms"] = self.__extract_timestamp(
            data["timestamp"],
        )

        processed['platform'] = _unicodify(event['platform'])

        tags = _as_dict_safe(data.get('tags', None))
        extract_extra_tags(processed, tags)

        promoted_tags = {col: tags[col]
            for col in self.PROMOTED_TAGS
            if col in tags
        }
        processed["release"] = promoted_tags.get(
            "sentry:release",
            event.get("release"),
        )
        processed["environment"] = promoted_tags.get("environment")

        contexts = _as_dict_safe(data.get('contexts', None))
        extract_extra_contexts(processed, contexts)

        processed["dist"] = _unicodify(
            promoted_tags.get("sentry:dist",
            data.get("dist")),
        )

        user_data = {}
        extract_user(user_data, data.get("user", {}))
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

        if metadata is not None:
            processed['partition'] = metadata.partition
            processed['offset'] = metadata.offset

        return ProcessedMessage(
            action=action_type,
            data=[processed],
        )
