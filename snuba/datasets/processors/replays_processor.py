from __future__ import annotations

import dataclasses
import logging
import uuid
from datetime import datetime, timezone
from hashlib import md5
from typing import Any, Callable, Mapping, TypeVar

import rapidjson

from snuba import environment
from snuba.datasets.processors.rust_compat_processor import RustCompatProcessor
from snuba.processor import _collapse_uint16, _collapse_uint32
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
LOG_LEVELS = ["fatal", "error", "warning", "info", "debug"]


class ReplaysProcessor(RustCompatProcessor):
    def __init__(self) -> None:
        super().__init__("ReplaysProcessor")


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


def to_uint32(value: Any) -> int:
    return _collapse_or_err(_collapse_uint32, int(value))


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
