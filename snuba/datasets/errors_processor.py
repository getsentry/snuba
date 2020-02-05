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

    def _should_process(self, event: Mapping[str, Any]) -> bool:
        return event["data"].get("type") != "transaction"

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
        output["transaction_name"] = tags.get("transaction", "")

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
        if "trace_id" in transaction_ctx:
            output["trace_id"] = str(uuid.UUID(transaction_ctx["trace_id"]))
        if "span_id" in transaction_ctx:
            output["span_id"] = int(transaction_ctx["span_id"], 16)
