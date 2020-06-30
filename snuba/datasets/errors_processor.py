from typing import Any, Mapping, MutableMapping, Optional

import logging
import _strptime  # NOQA fixes _strptime deferred import issue
import uuid

from snuba.consumer import KafkaMessageMetadata
from snuba.datasets.events_format import extract_user
from snuba.datasets.events_processor_base import EventsProcessorBase, InsertEvent
from snuba.processor import (
    _as_dict_safe,
    _ensure_valid_ip,
    _unicodify,
)

logger = logging.getLogger(__name__)


class ErrorsProcessor(EventsProcessorBase):
    def __init__(self, promoted_tag_columns: Mapping[str, str]):
        self._promoted_tag_columns = promoted_tag_columns

    def extract_promoted_tags(
        self, output: MutableMapping[str, Any], tags: Mapping[str, Any],
    ) -> None:
        output.update(
            {
                col_name: _unicodify(tags.get(tag_name, None))
                for tag_name, col_name in self._promoted_tag_columns.items()
            }
        )

    def _should_process(self, event: InsertEvent) -> bool:
        return event["data"].get("type") != "transaction"

    def _extract_event_id(
        self, output: MutableMapping[str, Any], event: InsertEvent,
    ) -> None:
        output["event_id"] = str(uuid.UUID(event["event_id"]))
        output["event_string"] = event["event_id"]

    def extract_custom(
        self,
        output: MutableMapping[str, Any],
        event: InsertEvent,
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
        event: InsertEvent,
        tags: Mapping[str, Any],
        metadata: Optional[KafkaMessageMetadata] = None,
    ) -> None:
        output["release"] = tags.get("sentry:release")
        output["dist"] = tags.get("sentry:dist")
        output["user"] = tags.get("sentry:user", "") or ""
        # The table has an empty string default, but the events coming from eventstream
        # often have transaction_name set to NULL, so we need to replace that with
        # an empty string.
        output["transaction_name"] = tags.get("transaction", "") or ""

    def extract_contexts_custom(
        self,
        output: MutableMapping[str, Any],
        event: InsertEvent,
        contexts: Mapping[str, Any],
        metadata: Optional[KafkaMessageMetadata] = None,
    ) -> None:
        output["_contexts_flattened"] = ""

    def extract_promoted_contexts(
        self,
        output: MutableMapping[str, Any],
        contexts: Mapping[str, Any],
        tags: Mapping[str, Any],
    ) -> None:
        transaction_ctx = contexts.get("trace") or {}
        if transaction_ctx.get("trace_id", None):
            output["trace_id"] = str(uuid.UUID(transaction_ctx["trace_id"]))
        if transaction_ctx.get("span_id", None):
            output["span_id"] = int(transaction_ctx["span_id"], 16)
