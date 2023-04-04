import re
from datetime import datetime, timedelta
from enum import Enum
from functools import wraps
from typing import (
    Any,
    Callable,
    MutableMapping,
    NamedTuple,
    Pattern,
    Sequence,
    TypeVar,
    Union,
    cast,
)

import _strptime  # NOQA fixes _strptime deferred import issue
from dateutil.parser import parse as dateutil_parse

from snuba.query.schema import CONDITION_OPERATORS
from snuba.utils.metrics.timer import Timer


def qualified_column(column_name: str, alias: str = "") -> str:
    """
    Returns a column in the form "table.column" if the table is not
    empty. If the table is empty it returns the column itself.
    """
    return column_name if not alias else f"{alias}.{column_name}"


def parse_datetime(value: str, alignment: int = 1) -> datetime:
    dt = dateutil_parse(value, ignoretz=True).replace(microsecond=0)
    return dt - timedelta(seconds=(dt - dt.min).seconds % alignment)


def is_condition(cond_or_list: Sequence[Any]) -> bool:
    return (
        # A condition is:
        # a 3-tuple
        len(cond_or_list) == 3
        and
        # where the middle element is an operator
        cond_or_list[1] in CONDITION_OPERATORS
        and
        # and the first element looks like a column name or expression
        isinstance(cond_or_list[0], (str, tuple, list))
    )


def tuplify(nested: Any) -> Any:
    if isinstance(nested, (list, tuple)):
        return tuple(tuplify(child) for child in nested)
    return nested


F = TypeVar("F", bound=Callable[..., Any])


def time_request(name: str) -> Callable[[F], F]:
    def decorator(func: F) -> F:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            kwargs["timer"] = Timer(name)
            return func(*args, **kwargs)

        return cast(F, wrapper)

    return decorator


class Part(NamedTuple):
    name: str
    date: datetime
    retention_days: int


class PartSegment(Enum):
    RETENTION_DAYS = "retention_days"
    DATE = "date"


re_cache: MutableMapping[str, Pattern[Any]] = {}


def decode_part_str(part_str: str, partition_format: Sequence[PartSegment]) -> Part:
    def get_re(format: Sequence[PartSegment]) -> Pattern[Any]:
        cache_key = ",".join([segment.value for segment in format])

        PARTSEGMENT_RE = {
            PartSegment.DATE: "('(?P<date>\d{4}-\d{2}-\d{2})')",
            PartSegment.RETENTION_DAYS: "(?P<retention_days>\d+)",
        }

        SEP = ",\s*"

        try:
            return re_cache[cache_key]
        except KeyError:
            re_cache[cache_key] = re.compile(
                f"\({SEP.join([PARTSEGMENT_RE[s] for s in partition_format])}\)"
            )

        return re_cache[cache_key]

    match = get_re(partition_format).match(part_str)

    if not match:
        raise ValueError("Unknown part name/format: " + str(part_str))

    date_str = match.group("date")
    retention_days = match.group("retention_days")

    if date_str and retention_days:
        return Part(
            part_str, datetime.strptime(date_str, "%Y-%m-%d"), int(retention_days)
        )

    else:
        raise ValueError("Unknown part name/format: " + str(part_str))


def force_bytes(s: Union[bytes, str]) -> bytes:
    if isinstance(s, bytes):
        return s
    elif isinstance(s, str):
        return s.encode("utf-8", "replace")
    else:
        raise TypeError(f"cannot convert {type(s).__name__} to bytes")
