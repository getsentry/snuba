import ipaddress
import re

from datetime import datetime
from enum import Enum
from hashlib import md5
import simplejson as json
from typing import Any, NamedTuple, Optional, Sequence, Union

from snuba.util import force_bytes

HASH_RE = re.compile(r"^[0-9a-f]{32}$", re.IGNORECASE)
MAX_UINT32 = 2 ** 32 - 1


class ProcessorAction(Enum):
    INSERT = 0
    REPLACE = 1


class ProcessedMessage(NamedTuple):
    action: ProcessorAction
    data: Sequence[Any]


class MessageProcessor(object):
    """
    The Processor is responsible for converting an incoming message body from the
    event stream into a row or statement to be inserted or executed against clickhouse.
    """

    def process_message(self, message, metadata=None,) -> Optional[ProcessedMessage]:
        raise NotImplementedError


class InvalidMessageType(Exception):
    pass


class InvalidMessageVersion(Exception):
    pass


def _as_dict_safe(value):
    if value is None:
        return {}
    if isinstance(value, dict):
        return value
    rv = {}
    for item in value:
        if item is not None:
            rv[item[0]] = item[1]
    return rv


def _collapse_uint32(n):
    if (n is None) or (n < 0) or (n > MAX_UINT32):
        return None
    return n


def _boolify(s):
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


def _floatify(s):
    if not s:
        return None

    if isinstance(s, float):
        return s

    try:
        s = float(s)
    except (ValueError, TypeError):
        return None
    else:
        return s

    return None


def _unicodify(s):
    if s is None:
        return None

    if isinstance(s, dict) or isinstance(s, list):
        return json.dumps(s)

    return str(s).encode("utf8", errors="backslashreplace").decode("utf8")


def _hashify(h):
    if HASH_RE.match(h):
        return h
    return md5(force_bytes(h)).hexdigest()


def _ensure_valid_date(dt):
    if dt is None:
        return None
    seconds = (dt - datetime(1970, 1, 1)).total_seconds()
    if _collapse_uint32(seconds) is None:
        return None
    return dt


def _ensure_valid_ip(
    ip: Any,
) -> Optional[Union[ipaddress.IPv4Address, ipaddress.IPv6Address]]:
    """
    IP addresses in e.g. `user.ip_address` might be invalid due to PII stripping.
    """

    ip = _unicodify(ip)
    if ip:
        try:
            return ipaddress.ip_address(ip)
        except ValueError:
            pass

    return None
