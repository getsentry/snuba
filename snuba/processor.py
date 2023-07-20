import ipaddress
import re
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from enum import Enum
from hashlib import md5
from typing import (
    Any,
    Dict,
    FrozenSet,
    Iterable,
    MutableMapping,
    NamedTuple,
    Optional,
    Sequence,
    Tuple,
    TypedDict,
    TypeVar,
    Union,
)

import simplejson as json

from snuba.consumers.types import KafkaMessageMetadata
from snuba.util import force_bytes
from snuba.utils.serializable_exception import SerializableException
from snuba.writer import WriterTableRow

HASH_RE = re.compile(r"^[0-9a-f]{32}$", re.IGNORECASE)
MAX_UINT16 = 2**16 - 1
MAX_UINT32 = 2**32 - 1
NIL_UUID = "00000000-0000-0000-0000-000000000000"


class InsertBatch(NamedTuple):
    rows: Sequence[WriterTableRow]
    origin_timestamp: Optional[datetime]
    sentry_received_timestamp: Optional[datetime] = None


# Indicates that we need an encoder that will interpolate
# aggregate function calls defined in
# https://clickhouse.com/docs/en/sql-reference/data-types/aggregatefunction/
class AggregateInsertBatch(InsertBatch):
    pass


class ReplacementBatch(NamedTuple):
    key: str
    values: Sequence[Any]


ProcessedMessage = Union[InsertBatch, ReplacementBatch]


class MessageProcessor(ABC):
    """
    The Processor is responsible for converting an incoming message body from the
    event stream into a row or statement to be inserted or executed against clickhouse.
    """

    @abstractmethod
    def process_message(
        self, message: Any, metadata: KafkaMessageMetadata
    ) -> Optional[ProcessedMessage]:
        raise NotImplementedError


class InvalidMessageType(SerializableException):
    pass


class InvalidMessageVersion(SerializableException):
    pass


class ReplacementType(str, Enum):
    START_DELETE_GROUPS = "start_delete_groups"
    START_MERGE = "start_merge"
    START_UNMERGE = "start_unmerge"
    START_UNMERGE_HIERARCHICAL = "start_unmerge_hierarchical"
    START_DELETE_TAG = "start_delete_tag"
    END_DELETE_GROUPS = "end_delete_groups"
    END_MERGE = "end_merge"
    END_UNMERGE = "end_unmerge"
    END_UNMERGE_HIERARCHICAL = "end_unmerge_hierarchical"
    END_DELETE_TAG = "end_delete_tag"
    TOMBSTONE_EVENTS = "tombstone_events"
    REPLACE_GROUP = "replace_group"
    EXCLUDE_GROUPS = "exclude_groups"


REPLACEMENT_EVENT_TYPES: FrozenSet[ReplacementType] = frozenset(
    ReplacementType.__members__.values()
)


class InsertEvent(TypedDict):
    group_id: Optional[int]
    event_id: str
    organization_id: int
    project_id: int
    message: str
    platform: str
    datetime: str  # snuba.settings.PAYLOAD_DATETIME_FORMAT
    data: MutableMapping[str, Any]
    primary_hash: str  # empty string represents None
    retention_days: int


TKey = TypeVar("TKey")
TValue = TypeVar("TValue")


def _as_dict_safe(
    value: Union[
        None, Iterable[Optional[Tuple[Optional[TKey], TValue]]], Dict[TKey, TValue]
    ],
) -> MutableMapping[TKey, TValue]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return value
    rv = {}
    for item in value:
        if item is not None and item[0] is not None:
            rv[item[0]] = item[1]
    return rv


def _collapse_uint16(n: Any) -> Optional[int]:
    if n is None:
        return None

    i = int(n)
    if (i < 0) or (i > MAX_UINT16):
        return None

    return i


def _collapse_uint32(n: Any) -> Optional[int]:
    if n is None:
        return None

    i = int(n)
    if (i < 0) or (i > MAX_UINT32):
        return None

    return i


def _boolify(s: Any) -> Optional[bool]:
    if s is None:
        return None

    if isinstance(s, bool):
        return s

    s = _unicodify(s)

    if s in ("yes", "true", "1"):
        return True
    elif s in ("false", "no", "0"):
        return False

    return None


def _unicodify(s: Any) -> Optional[str]:
    if s is None:
        return None

    if isinstance(s, dict) or isinstance(s, list):
        return json.dumps(s)

    return str(s).encode("utf8", errors="backslashreplace").decode("utf8")


def _hashify(h: str) -> str:
    if HASH_RE.match(h):
        return h
    return md5(force_bytes(h)).hexdigest()


epoch = datetime(1970, 1, 1)


def _ensure_valid_date(dt: Optional[datetime]) -> Optional[datetime]:
    if dt is None:
        return None
    seconds = (dt - epoch).total_seconds()
    if _collapse_uint32(seconds) is None:
        return None
    return epoch + timedelta(seconds=seconds)


def _ensure_valid_ip(
    ip: Any,
) -> Optional[Union[ipaddress.IPv4Address, ipaddress.IPv6Address]]:
    """
    IP addresses in e.g. `user.ip_address` might be invalid due to PII stripping.
    """

    ip = _unicodify(ip)
    if ip:
        try:
            ip_address = ipaddress.ip_address(ip)
            # Looking into ip_address code, it can either return one of the
            # two or raise. Anyway, if we received anything else the places where
            # we use this method would fail.
            if not isinstance(
                ip_address, (ipaddress.IPv4Address, ipaddress.IPv6Address)
            ):
                return None
            return ip_address
        except ValueError:
            pass

    return None
