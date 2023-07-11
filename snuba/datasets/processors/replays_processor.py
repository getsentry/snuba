from __future__ import annotations

import dataclasses
import logging
import uuid
from datetime import datetime, timezone
from functools import partial
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
    _collapse_uint16,
    _collapse_uint32,
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
MAX_CLICK_EVENTS = 20

USER_FIELDS_PRECEDENCE = ("user_id", "username", "email", "ip_address")


class ReplaysProcessor(DatasetMessageProcessor):
    def __extract_urls(self, replay_event: ReplayEventDict) -> list[str]:
        return to_typed_list(
            to_string, to_capped_list("urls", replay_event.get("urls"))
        )

    def __process_trace_ids(self, trace_ids: Any) -> list[str]:
        return to_typed_list(to_uuid, to_capped_list("trace_ids", trace_ids))

    def __process_error_ids(self, error_ids: Any) -> list[str]:
        return to_typed_list(to_uuid, to_capped_list("error_ids", error_ids))

    def _process_base_replay_event_values(
        self, processed: MutableMapping[str, Any], replay_event: ReplayEventDict
    ) -> None:
        processed["replay_id"] = to_uuid(replay_event["replay_id"])
        processed["segment_id"] = maybe(to_uint16, replay_event.get("segment_id"))
        processed["timestamp"] = default(
            lambda: datetime.now(timezone.utc),
            maybe(to_datetime, replay_event.get("timestamp")),
        )
        processed["replay_start_timestamp"] = maybe(
            to_datetime, replay_event.get("replay_start_timestamp")
        )
        processed["urls"] = self.__extract_urls(replay_event)
        processed["trace_ids"] = self.__process_trace_ids(replay_event.get("trace_ids"))
        processed["error_ids"] = self.__process_error_ids(replay_event.get("error_ids"))
        processed["release"] = maybe(to_string, replay_event.get("release"))
        processed["environment"] = maybe(to_string, replay_event.get("environment"))
        processed["dist"] = maybe(to_string, replay_event.get("dist"))
        processed["platform"] = maybe(to_string, replay_event["platform"])
        processed["replay_type"] = maybe(
            to_enum(["buffer", "session", "error"]), replay_event.get("replay_type")
        )

        # Archived can only be 1 or null.
        processed["is_archived"] = (
            1 if replay_event.get("is_archived") is True else None
        )

    def _process_tags(
        self, processed: MutableMapping[str, Any], replay_event: ReplayEventDict
    ) -> None:
        tags = process_tags_object(replay_event.get("tags"))

        # we have to set title to empty string as it is non-nullable,
        # and on clickhouse 20 this throws an error.
        processed["title"] = tags.transaction or ""
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
        processed["os_name"] = maybe(to_string, os_context.get("name"))
        processed["os_version"] = maybe(to_string, os_context.get("version"))

        browser_context = contexts.get("browser", {})
        processed["browser_name"] = maybe(to_string, browser_context.get("name"))
        processed["browser_version"] = maybe(to_string, browser_context.get("version"))

        device_context = contexts.get("device", {})
        processed["device_name"] = maybe(to_string, device_context.get("name"))
        processed["device_brand"] = maybe(to_string, device_context.get("brand"))
        processed["device_family"] = maybe(to_string, device_context.get("family"))
        processed["device_model"] = maybe(to_string, device_context.get("model"))

        replay = contexts.get("replay", {})
        processed["error_sample_rate"] = maybe(float, replay.get("error_sample_rate"))
        processed["session_sample_rate"] = maybe(
            float, replay.get("session_sample_rate")
        )

    def _process_sdk(
        self, processed: MutableMapping[str, Any], replay_event: ReplayEventDict
    ) -> None:
        sdk = replay_event.get("sdk", None) or {}
        processed["sdk_name"] = maybe(to_string, sdk.get("name"))
        processed["sdk_version"] = maybe(to_string, sdk.get("version"))

    def _process_kafka_metadata(
        self, metadata: KafkaMessageMetadata, processed: MutableMapping[str, Any]
    ) -> None:
        processed["partition"] = metadata.partition
        processed["offset"] = metadata.offset

    def _process_event_hash(
        self, processed: MutableMapping[str, Any], replay_event: ReplayEventDict
    ) -> None:
        event_hash = replay_event.get("event_hash")
        if event_hash is None:
            event_hash = segment_id_to_event_hash(replay_event["segment_id"])

        processed["event_hash"] = str(uuid.UUID(event_hash))

    def process_message(
        self, message: Mapping[Any, Any], metadata: KafkaMessageMetadata
    ) -> Optional[ProcessedMessage]:
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

        if replay_event["type"] == "replay_actions":
            actions = process_replay_actions(replay_event, processed, metadata)
            return InsertBatch(actions, None)
        else:
            # The following helper functions should be able to be applied in any order.
            # At time of writing, there are no reads of the values in the `processed`
            # dictionary to inform values in other functions.
            # Ideally we keep continue that rule
            self._process_base_replay_event_values(processed, replay_event)
            self._process_tags(processed, replay_event)
            self._process_sdk(processed, replay_event)
            self._process_kafka_metadata(metadata, processed)

            # the following operation modifies the event_dict and is therefore *not*
            # order-independent
            self._process_user(processed, replay_event)
            self._process_event_hash(processed, replay_event)
            self._process_contexts(processed, replay_event)
            return InsertBatch([processed], None)


def process_replay_actions(
    payload: Mapping[Any, Any],
    processed: Mapping[Any, Any],
    metadata: KafkaMessageMetadata,
) -> list[dict[str, Any]]:
    """Process replay_actions message type."""
    return [
        {
            # Primary-key.
            "project_id": processed["project_id"],
            "timestamp": raise_on_null(
                "timestamp", maybe(to_datetime, click["timestamp"])
            ),
            "replay_id": to_uuid(payload["replay_id"]),
            "segment_id": None,
            "event_hash": click["event_hash"],
            # Default values for non-nullable columns.
            "trace_ids": [],
            "error_ids": [],
            "urls": [],
            "platform": "javascript",
            "user": None,
            "sdk_name": None,
            "sdk_version": None,
            # Kafka columns.
            "retention_days": processed["retention_days"],
            "partition": metadata.partition,
            "offset": metadata.offset,
            # DOM Index fields.
            "click_node_id": _collapse_or_err(_collapse_uint32, int(click["node_id"])),
            "click_tag": to_string(click["tag"])[:32],
            "click_id": to_string(click["id"])[:64],
            "click_class": to_typed_list(
                partial(to_capped_string, 64), click["class"][:10]
            ),
            "click_text": to_string(click["text"])[:1024],
            "click_role": to_string(click["role"])[:32],
            "click_alt": to_string(click["alt"])[:64],
            "click_testid": to_string(click["testid"])[:64],
            "click_aria_label": to_string(click["aria_label"])[:64],
            "click_title": to_string(click["title"])[:64],
            "click_is_dead": to_uint1(click["is_dead"]),
            "click_is_rage": to_uint1(click["is_rage"]),
        }
        for click in payload["clicks"][:MAX_CLICK_EVENTS]
    ]


T = TypeVar("T")
U = TypeVar("U")


def segment_id_to_event_hash(segment_id: int | None) -> str:
    if segment_id is None:
        # Rows with null segment_id fields are considered "out of band" meaning they do not
        # originate from the SDK and do not relate to a specific segment.
        #
        # For example: archive requests.
        return str(uuid.uuid4())
    else:
        segment_id_bytes = force_bytes(str(segment_id))
        segment_hash = md5(segment_id_bytes).hexdigest()
        return to_uuid(segment_hash)


def default(default: Callable[[], T], value: T | None) -> T:
    """Return a default value only if the given value was null.

    Falsey types such as 0, "", False, [], {} are returned.
    """
    return default() if value is None else value


def maybe(into: Callable[[T], U], value: T | None) -> U | None:
    """Optionally return a processed value."""
    return None if value is None else into(value)


def to_datetime(value: Any) -> datetime:
    """Return a datetime instance or err.

    Datetimes for the replays schema standardize on 32 bit dates.
    """
    return _timestamp_to_datetime(_collapse_or_err(_collapse_uint32, int(value)))


def to_uint1(value: Any) -> int:
    int_value = int(value)
    if int_value == 0 or int_value == 1:
        return int_value
    else:
        raise ValueError("Value must be 0 or 1")


def to_uint16(value: Any) -> int:
    return _collapse_or_err(_collapse_uint16, int(value))


def to_string(value: Any) -> str:
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


def to_capped_string(capacity: int, value: Any) -> str:
    """Return a capped string."""
    return to_string(value)[:capacity]


def to_enum(enumeration: list[str]) -> Callable[[Any], str | None]:
    def inline(value: Any) -> str | None:
        for enum in enumeration:
            if value == enum:
                return enum
        return None

    return inline


def to_capped_list(metric_name: str, value: Any) -> list[Any]:
    """Return a list of values capped to the maximum allowable limit."""
    return _capped_list(metric_name, default(list, maybe(_is_list, value)))


def to_typed_list(callable: Callable[[Any], T], values: list[Any]) -> list[T]:
    return list(map(callable, filter(lambda value: value is not None, values)))


def to_uuid(value: Any) -> str:
    """Return a stringified uuid or err."""
    return str(uuid.UUID(str(value)))


def raise_on_null(field: str, value: Any) -> Any:
    if value is None:
        raise ValueError(f"Missing data for required field: {field}")
    return value


def _is_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    else:
        raise TypeError(
            f"Invalid type specified.  Expected list; received {type(value)} with a value of "
            f"{value}"
        )


def _encode_utf8(value: str) -> str:
    """Return a utf-8 encoded string."""
    return value.encode("utf8", errors="backslashreplace").decode("utf8")


def _capped_list(metric_name: str, value: list[Any]) -> list[Any]:
    """Return a list with a maximum configured length."""
    if len(value) > LIST_ELEMENT_LIMIT:
        metrics.increment(f'"{metric_name}" exceeded maximum length.')

    return value[:LIST_ELEMENT_LIMIT]


def _collapse_or_err(callable: Callable[[int], int | None], value: int) -> int:
    """Return the integer or error if it overflows."""
    if callable(value) is None:
        # This exception can only be triggered through abuse.  We choose not to suppress these
        # exceptions in favor of identifying the origin.
        raise ValueError(f'Integer "{value}" overflowed.')
    else:
        return value


def _timestamp_to_datetime(timestamp: int) -> datetime:
    """Convert an integer timestamp to a timezone-aware utc datetime instance."""
    return datetime.fromtimestamp(timestamp, tz=timezone.utc)


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
    tags = _capped_list("tags", normalize_tags(value))

    keys = []
    values = []
    transaction = None

    for key, value in tags:
        # Keys and values are stored as optional strings regardless of their input type.
        parsed_key, parsed_value = to_string(key), maybe(to_string, value)

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
