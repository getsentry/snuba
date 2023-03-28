import inspect
import re
from datetime import datetime, timedelta
from enum import Enum
from functools import partial, wraps
from typing import (
    Any,
    Callable,
    Mapping,
    MutableMapping,
    NamedTuple,
    Optional,
    Pattern,
    Sequence,
    TypeVar,
    Union,
    cast,
)

import _strptime  # NOQA fixes _strptime deferred import issue
import sentry_sdk
from dateutil.parser import parse as dateutil_parse

from snuba import settings
from snuba.query.schema import CONDITION_OPERATORS
from snuba.utils.metrics import MetricsBackend
from snuba.utils.metrics.timer import Timer
from snuba.utils.metrics.types import Tags


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


def create_metrics(
    prefix: str,
    tags: Optional[Tags] = None,
    sample_rates: Optional[Mapping[str, float]] = None,
) -> MetricsBackend:
    """Create a DogStatsd object if DOGSTATSD_HOST and DOGSTATSD_PORT are defined,
    with the specified prefix and tags. Return a DummyMetricsBackend otherwise.
    Prefixes must start with `snuba.<category>`, for example: `snuba.processor`.
    """
    host: Optional[str] = settings.DOGSTATSD_HOST
    port: Optional[int] = settings.DOGSTATSD_PORT

    if settings.TESTING:
        from snuba.utils.metrics.backends.testing import TestingMetricsBackend

        return TestingMetricsBackend()
    elif host is None and port is None:
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
        sample_rates,
    )


def with_span(op: str = "function") -> Callable[[F], F]:
    """Wraps a function call in a Sentry AM span"""

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
