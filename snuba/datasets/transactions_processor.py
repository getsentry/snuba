from datetime import datetime

import ipaddress
import uuid

from snuba.processor import (
    _as_dict_safe,
    MessageProcessor,
    _ensure_valid_date,
    _unicodify
)
from snuba.datasets.events_processor import (
    extract_base,
    extract_extra_contexts,
    extract_extra_tags,
    extract_user
)


class TransactionsMessageProcessor(MessageProcessor):
    def __extract_timestamp(self, field):
        timestamp = _ensure_valid_date(datetime.fromtimestamp(field))
        milliseconds = int(timestamp.microsecond / 1000)
        return (timestamp, milliseconds)

    def process_message(self, message, metadata=None):
        action_type = self.INSERT
        processed = {'deleted': 0}

        if not isinstance(message, (list, tuple)) and len(message) >= 2:
            return None
        version = message[0]
        if version not in (0, 1, 2):
            return None
        type_, event = message[1:3]
        if type_ != 'insert':
            return None

        message = event

        data = message.get("data", {})
        event_type = data.get("type")
        if event_type != "transaction":
            return None

        extract_base(processed, message)

        transaction_ctx = data["contexts"]["trace"]
        trace_id = transaction_ctx["trace_id"]
        processed["event_id"] = str(uuid.UUID(processed["event_id"]))
        processed["trace_id"] = str(uuid.UUID(trace_id))
        processed["span_id"] = int(transaction_ctx["span_id"], 16)

        processed["transaction_name"] = _unicodify(data["transaction"])

        # TODO: what goes here ?
        processed["transaction_op"] = ""

        processed["start_ts"], processed["start_ms"] = self.__extract_timestamp(
            data["start_timestamp"],
        )

        processed["finish_ts"], processed["finish_ms"] = self.__extract_timestamp(
            data["timestamp"],
        )

        processed['platform'] = _unicodify(message['platform'])

        promoted_tag_columns = [
            "environment",
            "release",
        ]

        tags = _as_dict_safe(data.get('tags', None))
        extract_extra_tags(processed, tags)

        processed.update({col: _unicodify(tags.get(col, None))
            for col in promoted_tag_columns
        })

        contexts = _as_dict_safe(data.get('contexts', None))
        extract_extra_contexts(processed, contexts)

        processed["dist"] = _unicodify(data.get("dist", None))
        user_data = {}
        extract_user(user_data, data.get("user", {}))
        processed["user_name"] = user_data.get("username")
        processed["user_id"] = user_data.get("user_id")
        processed["user_email"] = user_data.get("email")
        ip_address = user_data.get("ip_address")
        if ip_address:
            ip_address = ipaddress.ip_address(ip_address)
            if ip_address.version == 4:
                processed["ip_address_v4"] = str(ip_address)
            elif ip_address.version == 6:
                processed["ip_address_v6"] = str(ip_address)

        if metadata is not None:
            processed['offset'] = metadata.offset
            processed['partition'] = metadata.partition

        return(action_type, processed)
