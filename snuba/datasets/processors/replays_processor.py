from __future__ import annotations

import dataclasses
import logging
import uuid
from datetime import datetime
from hashlib import md5
from typing import Any, Callable, Mapping, MutableMapping, Optional, TypeVar

import rapidjson

from snuba import environment
from snuba.consumers.types import KafkaMessageMetadata
from snuba.datasets.events_format import EventTooOld, enforce_retention, extract_user
from snuba.datasets.processors import DatasetMessageProcessor
from snuba.processor import (
    InsertBatch,
    ProcessedMessage,
    _ensure_valid_date,
    _ensure_valid_ip,
)
from snuba.util import force_bytes
from snuba.utils.metrics.wrapper import MetricsWrapper

logger = logging.getLogger(__name__)

metrics = MetricsWrapper(environment.metrics, "replays.processor")

ReplayEventDict = Mapping[Any, Any]
RetentionDays = int

# Limit for error_ids / trace_ids / urls array elements
LIST_ELEMENT_LIMIT = 1000

USER_FIELDS_PRECEDENCE = ("user_id", "username", "email", "ip_address")


class ReplaysProcessor(DatasetMessageProcessor):
    def __extract_timestamp(self, field: int) -> datetime:
        timestamp = _ensure_valid_date(datetime.utcfromtimestamp(field))
        if timestamp is None:
            timestamp = datetime.utcnow()
        return timestamp

    def __extract_started_at(self, started_at: Optional[int]) -> Optional[datetime]:
        if started_at is None:
            return None

        timestamp = _ensure_valid_date(datetime.utcfromtimestamp(started_at))
        if timestamp:
            return timestamp
        else:
            raise TypeError("Missing data for replay_start_timestamp column.")

    def __extract_urls(self, replay_event: ReplayEventDict) -> list[str]:
        if "url" in replay_event:
            # Backwards compat for non-public, pre-alpha javascript SDK.
            return self.__extract_url(replay_event)
        elif "urls" in replay_event:
            # Latest SDK input.
            urls = replay_event.get("urls")
            if isinstance(urls, list):
                return list(
                    filter(
                        None,
                        [maybe(stringify, url) for url in urls[:LIST_ELEMENT_LIMIT]],
                    )
                )
            else:
                return []
        else:
            # Malformed event catch all.
            return []

    def __extract_url(self, replay_event: ReplayEventDict) -> list[str]:
        request = replay_event.get("request", {})
        if not isinstance(request, dict):
            return []

        url = request.get("url")
        return [stringify(url)] if isinstance(url, str) else []

    def __process_trace_ids(self, trace_ids: list[str] | None) -> list[str]:
        if not trace_ids:
            return []
        if len(trace_ids) > LIST_ELEMENT_LIMIT:
            metrics.increment("trace_ids exceeded list limit")

        return [str(uuid.UUID(t)) for t in trace_ids[:LIST_ELEMENT_LIMIT]]

    def __process_error_ids(self, error_ids: list[str] | None) -> list[str]:
        if not error_ids:
            return []
        if len(error_ids) > LIST_ELEMENT_LIMIT:
            metrics.increment("error_ids exceeded list limit")

        return [str(uuid.UUID(e)) for e in error_ids[:LIST_ELEMENT_LIMIT]]

    def _process_base_replay_event_values(
        self, processed: MutableMapping[str, Any], replay_event: ReplayEventDict
    ) -> None:
        processed["replay_id"] = str(uuid.UUID(replay_event["replay_id"]))
        processed["segment_id"] = replay_event["segment_id"]
        processed["trace_ids"] = self.__process_trace_ids(replay_event.get("trace_ids"))

        processed["timestamp"] = self.__extract_timestamp(
            replay_event["timestamp"],
        )
        processed["replay_start_timestamp"] = self.__extract_started_at(
            replay_event.get("replay_start_timestamp"),
        )
        processed["urls"] = self.__extract_urls(replay_event)
        processed["release"] = maybe(stringify, replay_event.get("release"))
        processed["environment"] = maybe(stringify, replay_event.get("environment"))
        processed["dist"] = maybe(stringify, replay_event.get("dist"))
        processed["platform"] = maybe(stringify, replay_event["platform"])

        processed["error_ids"] = self.__process_error_ids(replay_event.get("error_ids"))

        # Archived can only be 1 or null.
        processed["is_archived"] = 1 if replay_event.get("is_archived") else None

    def _process_tags(
        self, processed: MutableMapping[str, Any], replay_event: ReplayEventDict
    ) -> None:
        tags = process_tags_object(replay_event.get("tags"))
        processed["title"] = tags.transaction
        processed["tags.key"] = tags.keys
        processed["tags.value"] = tags.values

    def _add_user_column(
        self,
        processed: MutableMapping[str, Any],
        user_data: MutableMapping[str, Any],
    ) -> None:
        # TODO: this could be optimized / removed

        for field in USER_FIELDS_PRECEDENCE:
            if field in user_data and user_data[field]:
                processed["user"] = user_data[field]
                return

    def _process_user(
        self, processed: MutableMapping[str, Any], replay_event: ReplayEventDict
    ) -> None:
        user_dict = replay_event.get("user") or {}
        user_data: MutableMapping[str, Any] = {}

        # "extract_user" calls "_unicodify" so we can be reasonably sure it has coerced the
        # strings correctly.
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

        self._add_user_column(processed, user_data)

    def _process_contexts(
        self, processed: MutableMapping[str, Any], replay_event: ReplayEventDict
    ) -> None:
        contexts = replay_event.get("contexts", {})
        if not contexts:
            return None

        os_context = contexts.get("os", {})
        processed["os_name"] = maybe(stringify, os_context.get("name"))
        processed["os_version"] = maybe(stringify, os_context.get("version"))

        browser_context = contexts.get("browser", {})
        processed["browser_name"] = maybe(stringify, browser_context.get("name"))
        processed["browser_version"] = maybe(stringify, browser_context.get("version"))

        device_context = contexts.get("device", {})
        processed["device_name"] = maybe(stringify, device_context.get("name"))
        processed["device_brand"] = maybe(stringify, device_context.get("brand"))
        processed["device_family"] = maybe(stringify, device_context.get("family"))
        processed["device_model"] = maybe(stringify, device_context.get("model"))

    def _process_sdk(
        self, processed: MutableMapping[str, Any], replay_event: ReplayEventDict
    ) -> None:
        sdk = replay_event.get("sdk", None) or {}
        processed["sdk_name"] = maybe(stringify, sdk.get("name"))
        processed["sdk_version"] = maybe(stringify, sdk.get("version"))

    def _process_kafka_metadata(
        self, metadata: KafkaMessageMetadata, processed: MutableMapping[str, Any]
    ) -> None:
        processed["partition"] = metadata.partition
        processed["offset"] = metadata.offset

    def _process_event_hash(
        self, processed: MutableMapping[str, Any], replay_event: ReplayEventDict
    ) -> None:
        # event_hash is used to uniquely identify a row within a replay
        # for our segment updates we'll simply hash the segment_id
        # for future additions, we'll use whatever unique identifier (e.g. event_id)
        # as the input to the hash

        segment_id_bytes = force_bytes(str((replay_event["segment_id"])))
        segment_hash = md5(segment_id_bytes).hexdigest()
        processed["event_hash"] = str(uuid.UUID(segment_hash))

    def process_message(
        self, message: Mapping[Any, Any], metadata: KafkaMessageMetadata
    ) -> Optional[ProcessedMessage]:
        try:
            replay_event = rapidjson.loads(bytes(message["payload"]))
            try:
                retention_days = enforce_retention(
                    message["retention_days"],
                    datetime.utcfromtimestamp(message["start_time"]),
                )
            except EventTooOld:
                return None

            processed: MutableMapping[str, Any] = {
                "retention_days": retention_days,
                "project_id": message["project_id"],
            }

            # # The following helper functions should be able to be applied in any order.
            # # At time of writing, there are no reads of the values in the `processed`
            # # dictionary to inform values in other functions.
            # # Ideally we keep continue that rule
            self._process_base_replay_event_values(processed, replay_event)
            self._process_tags(processed, replay_event)
            self._process_sdk(processed, replay_event)
            self._process_kafka_metadata(metadata, processed)

            # # the following operation modifies the event_dict and is therefore *not* order-independent
            self._process_user(processed, replay_event)
            self._process_event_hash(processed, replay_event)
            self._process_contexts(processed, replay_event)
            return InsertBatch([processed], None)
        except Exception:
            metrics.increment("consumer_error")
            raise


T = TypeVar("T")
U = TypeVar("U")


def maybe(into: Callable[[T], U], value: T | None) -> U | None:
    """Optionally return a processed value."""
    return None if value is None else into(value)


def stringify(value: Any) -> str:
    """Return a string or err.

    This function follows the lead of "snuba.processors._unicodify" and enforces UTF-8
    encoding.
    """
    if isinstance(value, (bool, dict, list)):
        result: str = rapidjson.dumps(value)
        return _encode_utf8(result)
    elif value is None:
        return ""
    else:
        return _encode_utf8(str(value))


def _encode_utf8(value: str) -> str:
    """Return a utf-8 encoded string."""
    return value.encode("utf8", errors="backslashreplace").decode("utf8")


# Tags processor.


@dataclasses.dataclass
class Tag:
    keys: list[str]
    values: list[str]
    transaction: str | None

    @classmethod
    def empty_set(cls) -> Tag:
        return cls([], [], None)


def process_tags_object(value: Any) -> Tag:
    if value is None:
        return Tag.empty_set()

    # Excess tags are trimmed.
    tags = capped_list("tags", normalize_tags(value))

    keys = []
    values = []
    transaction = None

    for key, value in tags:
        # Keys and values are stored as optional strings regardless of their input type.
        parsed_key, parsed_value = stringify(key), maybe(stringify, value)

        if key == "transaction":
            transaction = parsed_value
        elif parsed_value is not None:
            keys.append(parsed_key)
            values.append(parsed_value)

    return Tag(keys=keys, values=values, transaction=transaction)


def normalize_tags(value: Any) -> list[tuple[str, str]]:
    """Normalize tags payload to a single format."""
    if isinstance(value, dict):
        return _coerce_tags_dictionary(value)
    elif isinstance(value, list):
        return _coerce_tags_tuple_list(value)
    else:
        raise TypeError(f'Invalid tags type specified: "{type(value)}"')


def _coerce_tags_dictionary(tags: dict[str, Any]) -> list[tuple[str, str]]:
    """Return a list of tag tuples from an unspecified dictionary."""
    return [(key, value) for key, value in tags.items() if isinstance(key, str)]


def _coerce_tags_tuple_list(tags_list: list[Any]) -> list[tuple[str, str]]:
    """Return a list of tag tuples from an unspecified list of values."""
    return [
        (item[0], item[1])
        for item in tags_list
        if (isinstance(item, (list, tuple)) and len(item) == 2)
    ]


def capped_list(metric_name: str, value: list[Any]) -> list[Any]:
    """Return a list with a maximum configured length."""
    if len(value) > LIST_ELEMENT_LIMIT:
        metrics.increment(f'"{metric_name}" exceeded maximum length.')

    return value[:LIST_ELEMENT_LIMIT]
