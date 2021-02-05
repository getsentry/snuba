import ipaddress
import re
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from hashlib import md5
from typing import (
    Any,
    Dict,
    Iterable,
    MutableMapping,
    NamedTuple,
    Optional,
    Sequence,
    Tuple,
    TypeVar,
    Union,
)

import simplejson as json

from snuba.consumers.types import KafkaMessageMetadata
from snuba.util import force_bytes
from snuba.writer import WriterTableRow


HASH_RE = re.compile(r"^[0-9a-f]{32}$", re.IGNORECASE)
MAX_UINT16 = 2 ** 16 - 1
MAX_UINT32 = 2 ** 32 - 1
NIL_UUID = "00000000-0000-0000-0000-000000000000"


class InsertBatch(NamedTuple):
    rows: Sequence[WriterTableRow]


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


class InvalidMessageType(Exception):
    pass


class InvalidMessageVersion(Exception):
    pass


TKey = TypeVar("TKey")
TValue = TypeVar("TValue")


def _as_dict_safe(
    value: Union[None, Iterable[Tuple[TKey, TValue]], Dict[TKey, TValue]],
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


def _floatify(s: Any) -> Optional[float]:
    if s is None:
        return None

    if isinstance(s, float):
        return s

    try:
        return float(s)
    except (ValueError, TypeError):
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
            # two or raise. Anyway, if received anything else the places where
            # we use this method would fail.
            assert isinstance(
                ip_address, (ipaddress.IPv4Address, ipaddress.IPv6Address)
            )
        except ValueError:
            pass

    return None
