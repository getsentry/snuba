import inspect
import logging
import numbers
import re
from datetime import date, datetime, timedelta
from enum import Enum
from functools import partial, wraps
from typing import (
    Any,
    Callable,
    List,
    MutableMapping,
    NamedTuple,
    Optional,
    Pattern,
    Sequence,
    Tuple,
    TypeVar,
    Union,
    cast,
)

import _strptime  # NOQA fixes _strptime deferred import issue
import sentry_sdk
from dateutil.parser import parse as dateutil_parse

from snuba import settings
from snuba.clickhouse.escaping import escape_string
from snuba.query.schema import CONDITION_OPERATORS
from snuba.utils.metrics import MetricsBackend
from snuba.utils.metrics.timer import Timer
from snuba.utils.metrics.types import Tags

logger = logging.getLogger("snuba.util")


T = TypeVar("T")

# example partition name: "('2018-03-13 00:00:00', 90)"
PART_RE = r"\('(?P<timestamp>\d{4}-\d{2}-\d{2})',\s*(?P<retention>\d+)\)"

QUOTED_LITERAL_RE = re.compile(r"^'[\s\S]*'$")
SAFE_FUNCTION_RE = re.compile(r"-?[a-zA-Z_][a-zA-Z0-9_]*$")


def to_list(value: Union[T, List[T]]) -> List[T]:
    return value if isinstance(value, list) else [value]


def qualified_column(column_name: str, alias: str = "") -> str:
    """
    Returns a column in the form "table.column" if the table is not
    empty. If the table is empty it returns the column itself.
    """
    return column_name if not alias else f"{alias}.{column_name}"


def parse_datetime(value: str, alignment: int = 1) -> datetime:
    dt = dateutil_parse(value, ignoretz=True).replace(microsecond=0)
    return dt - timedelta(seconds=(dt - dt.min).seconds % alignment)


# TODO: Fix the type of Tuple concatenation when mypy supports it.
def is_function(column_expr: Any, depth: int = 0) -> Optional[Tuple[Any, ...]]:
    """
    Returns a 3-tuple of (name, args, alias) if column_expr is a function,
    otherwise None.

    A function expression is of the form:

        [func, [arg1, arg2]]  => func(arg1, arg2)

    If a string argument is followed by list arg, the pair of them is assumed
    to be a nested function call, with extra args to the outer function afterward.

        [func1, [func2, [arg1, arg2], arg3]]  => func1(func2(arg1, arg2), arg3)

    Although at the top level, there is no outer function call, and the optional
    3rd argument is interpreted as an alias for the entire expression.

        [func, [arg1], alias] => function(arg1) AS alias

    """
    if (
        isinstance(column_expr, (tuple, list))
        and len(column_expr) >= 2
        and isinstance(column_expr[0], str)
        and isinstance(column_expr[1], (tuple, list))
        and (depth > 0 or len(column_expr) <= 3)
    ):
        assert SAFE_FUNCTION_RE.match(column_expr[0])
        if len(column_expr) == 2:
            return tuple(column_expr) + (None,)
        else:
            return tuple(column_expr)
    else:
        return None


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


def escape_literal(
    value: Optional[Union[str, datetime, date, List[Any], Tuple[Any], numbers.Number]]
) -> str:
    """
    Escape a literal value for use in a SQL clause.
    """
    if isinstance(value, str):
        return escape_string(value)
    elif isinstance(value, datetime):
        value = value.replace(tzinfo=None, microsecond=0)
        return "toDateTime('{}', 'Universal')".format(value.isoformat())
    elif isinstance(value, date):
        return "toDate('{}', 'Universal')".format(value.isoformat())
    elif isinstance(value, (list, tuple)):
        return "({})".format(", ".join(escape_literal(v) for v in value))
    elif isinstance(value, numbers.Number):
        return str(value)
    elif value is None:
        return ""
    else:
        raise ValueError("Do not know how to escape {} for SQL".format(type(value)))


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


def decode_part_str(part_str: str, part_format: Sequence[PartSegment]) -> Part:
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
                f"\({SEP.join([PARTSEGMENT_RE[s] for s in part_format])}\)"
            )

        return re_cache[cache_key]

    match = get_re(part_format).match(part_str)

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


def create_metrics(prefix: str, tags: Optional[Tags] = None) -> MetricsBackend:
    """Create a DogStatsd object if DOGSTATSD_HOST and DOGSTATSD_PORT are defined,
    with the specified prefix and tags. Return a DummyMetricsBackend otherwise.
    Prefixes must start with `snuba.<category>`, for example: `snuba.processor`.
    """
    host = settings.DOGSTATSD_HOST
    port = settings.DOGSTATSD_PORT

    if host is None and port is None:
        from snuba.utils.metrics.backends.dummy import DummyMetricsBackend

        return DummyMetricsBackend()
    elif host is None or port is None:
        raise ValueError(
            f"DOGSTATSD_HOST and DOGSTATSD_PORT should both be None or not None. Found DOGSTATSD_HOST: {host}, DOGSTATSD_PORT: {port} instead."
        )

    from datadog import DogStatsd

    from snuba.utils.metrics.backends.datadog import DatadogMetricsBackend

    return DatadogMetricsBackend(
        partial(
            DogStatsd,
            host=host,
            port=port,
            namespace=prefix,
            constant_tags=[f"{key}:{value}" for key, value in tags.items()]
            if tags is not None
            else None,
        ),
    )


def with_span(op: str = "function") -> Callable[[F], F]:
    """ Wraps a function call in a Sentry AM span
    """

    def decorator(func: F) -> F:
        frame_info = inspect.stack()[1]
        filename = frame_info.filename

        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            with sentry_sdk.start_span(description=func.__name__, op=op) as span:
                span.set_data("filename", filename)
                return func(*args, **kwargs)

        return cast(F, wrapper)

    return decorator
