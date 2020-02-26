from typing import Any, Mapping, MutableMapping, Optional

import logging
import _strptime  # NOQA fixes _strptime deferred import issue
import uuid

from snuba.consumer import KafkaMessageMetadata
from snuba.datasets.events_format import (
    extract_extra_contexts,
    extract_user,
    flatten_nested_field,
)
from snuba.datasets.events_processor_base import EventsProcessorBase
from snuba.processor import (
    _as_dict_safe,
    _ensure_valid_ip,
    _unicodify,
)

logger = logging.getLogger(__name__)


class ErrorsProcessor(EventsProcessorBase):
    def _should_process(self, event: Mapping[str, Any]) -> bool:
        # This is to convince mypy that we actually return a bool
        data: Mapping[str, Any] = event["data"]
        return data.get("type") != "transaction"

    def _extract_event_id(
        self, output: MutableMapping[str, Any], event: Mapping[str, Any],
    ) -> None:
        output["event_id"] = str(uuid.UUID(event["event_id"]))
        output["event_string"] = event["event_id"]

    def extract_custom(
        self,
        output: MutableMapping[str, Any],
        event: Mapping[str, Any],
        metadata: Optional[KafkaMessageMetadata] = None,
    ) -> None:
        data = event.get("data", {})
        user_dict = data.get("user", data.get("sentry.interfaces.User", None)) or {}

        user_data: MutableMapping[str, Any] = {}
        extract_user(user_data, user_dict)
        output["user_name"] = user_data["username"]
        output["user_id"] = user_data["user_id"]
        output["user_email"] = user_data["email"]

        ip_address = _ensure_valid_ip(user_data["ip_address"])
        if ip_address:
            if ip_address.version == 4:
                output["ip_address_v4"] = str(ip_address)
            elif ip_address.version == 6:
                output["ip_address_v6"] = str(ip_address)

        contexts = _as_dict_safe(data.get("contexts", None))
        geo = user_dict.get("geo", {})
        if "geo" not in contexts and isinstance(geo, dict):
            contexts["geo"] = geo

        request = data.get("request", data.get("sentry.interfaces.Http", None)) or {}
        if "request" not in contexts and isinstance(request, dict):
            http = {}
            http["http_method"] = _unicodify(request.get("method", None))
            http_headers = _as_dict_safe(request.get("headers", None))
            http["http_referer"] = _unicodify(http_headers.get("Referer", None))
            contexts["request"] = http

        # _as_dict_safe may not return a reference to the entry in the data
        # dictionary in some cases.
        data["contexts"] = contexts

        output["message"] = _unicodify(event["message"])
        output["org_id"] = event["organization_id"]

    def extract_tags_custom(
        self,
        output: MutableMapping[str, Any],
        event: Mapping[str, Any],
        tags: Mapping[str, Any],
        metadata: Optional[KafkaMessageMetadata] = None,
    ) -> None:
        output["release"] = tags.get("sentry:release")
        output["dist"] = tags.get("sentry:dist")
        output["user"] = tags.get("sentry:user", "")
        # The table has an empty string default, but the events coming from eventstream
        # often have transaction_name set to NULL, so we need to replace that with
        # an empty string.
        output["transaction_name"] = tags.get("transaction", "") or ""

    def extract_contexts_custom(
        self,
        output: MutableMapping[str, Any],
        event: Mapping[str, Any],
        contexts: Mapping[str, Any],
        metadata: Optional[KafkaMessageMetadata] = None,
    ) -> None:
        key, value = extract_extra_contexts(contexts)
        output["_contexts_flattened"] = flatten_nested_field(key, value)

    def extract_promoted_contexts(
        self,
        output: MutableMapping[str, Any],
        contexts: Mapping[str, Any],
        tags: Mapping[str, Any],
    ) -> None:
        transaction_ctx = contexts.get("trace", {})
        if transaction_ctx.get("trace_id", None):
            output["trace_id"] = str(uuid.UUID(transaction_ctx["trace_id"]))
        if transaction_ctx.get("span_id", None):
            output["span_id"] = int(transaction_ctx["span_id"], 16)
