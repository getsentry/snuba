from datetime import datetime
from typing import Optional
from UUID import uuid

import logging
import _strptime  # NOQA fixes _strptime deferred import issue

from snuba import settings
from snuba.datasets.events_format import (
    enforce_retention,
    extract_base,
    extract_extra_contexts,
    extract_extra_tags,
    extract_user,
    flatten_nested_field,
    EventTooOld,
)
from snuba.datasets.events_processor_base import EventsProcessorBase
from snuba.processor import (
    _as_dict_safe,
    _boolify,
    _collapse_uint32,
    _ensure_valid_date,
    _ensure_valid_ip,
    _floatify,
    _hashify,
    _unicodify,
    InvalidMessageType,
    InvalidMessageVersion,
    MessageProcessor,
    ProcessorAction,
    ProcessedMessage,
)


logger = logging.getLogger("snuba.processor")


class ErrorsProcessor(EventsProcessorBase):
    def _should_process(self, event):
        event_type = event["data"].get("type")
        return event_type != "transaction"

    def _extract_event_id(self, output, message):
        output["event_id"] = str(uuid.UUID(message["event_id"]))
        output["event_string"] = message["event_id"]
        return output

    def extract_custom(self, output, message, data):
        user_dict = data.get("user", data.get("sentry.interfaces.User", None)) or {}
        user_data = {}
        extract_user(user_data, user_dict)
        ip_address = _ensure_valid_ip(user_data["ip_address"])

        if ip_address:
            if ip_address.version == 4:
                output["ip_address_v4"] = str(ip_address)
            elif ip_address.version == 6:
                output["ip_address_v6"] = str(ip_address)

        output["user_name"] = user_data["username"]
        output["user_id"] = user_data["user_id"]
        output["user_email"] = user_data["email"]

        transaction_ctx = data["contexts"].get("trace", {})
        if "trace_id" in transaction_ctx:
            output["trace_id"] = str(uuid.UUID(transaction_ctx["trace_id"]))
        if "span_id" in transaction_ctx:
            output["span_id"] = int(transaction_ctx["span_id"], 16)

        output["message"] = _unicodify(message["message"])

    def extract_custom_post_tags(self, output, message, tags, metadata=None):
        output["release"] = tags.get("sentry:release")
        output["dist"] = tags.get("sentry:dist")
        output["user"] = tags.get("sentry:user", "")

    def extract_custom_post_contexts(self, output, message, contexts, metadata=None):
        output["_contexts_flattened"] = flatten_nested_field(
            output["contexts.key"], output["contexts.value"]
        )
