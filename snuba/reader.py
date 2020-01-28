from __future__ import annotations

import itertools
import re
from abc import ABC, abstractmethod
from datetime import datetime
from typing import (
    TYPE_CHECKING,
    Any,
    Generic,
    Mapping,
    MutableMapping,
    Optional,
    Sequence,
    TypeVar,
)

from dateutil.tz import tz


if TYPE_CHECKING:
    from mypy_extensions import TypedDict

    Column = TypedDict("Column", {"name": str, "type": str})
    Row = MutableMapping[str, Any]
    Result = TypedDict(
        "Result",
        {"meta": Sequence[Column], "data": Sequence[Row], "totals": Row},
        total=False,
    )
else:
    Result = MutableMapping[str, Any]

TQuery = TypeVar("TQuery")


class Reader(ABC, Generic[TQuery]):
    @abstractmethod
    def execute(
        self,
        query: TQuery,
        settings: Optional[Mapping[str, str]] = None,
        query_id: Optional[str] = None,
        with_totals: bool = False,
    ) -> Result:
        """Execute a query."""
        raise NotImplementedError


DATE_TYPE_RE = re.compile(r"(Nullable\()?Date\b")
DATETIME_TYPE_RE = re.compile(r"(Nullable\()?DateTime\b")

UUID_TYPE_RE = re.compile(r"(Nullable\()?UUID\b")


def transform_columns(result: Result) -> Result:
    """
    Converts Clickhouse results into formatted strings. Specifically:
    - timezone-naive date and datetime values returned by ClickHouse
       into ISO 8601 formatted strings (including the UTC offset.)
    - UUID objects into strings
    """

    def iterate_rows():
        if "totals" in result:
            return itertools.chain(result["data"], [result["totals"]])
        else:
            return iter(result["data"])

    columns = {col["name"]: col["type"] for col in result["meta"]}
    for col_name, col_type in columns.items():
        if DATETIME_TYPE_RE.match(col_type):
            for row in iterate_rows():
                if (
                    row[col_name] is not None
                ):  # The column value can be null/None at times.
                    row[col_name] = row[col_name].replace(tzinfo=tz.tzutc()).isoformat()
        elif DATE_TYPE_RE.match(col_type):
            for row in iterate_rows():
                if (
                    row[col_name] is not None
                ):  # The column value can be null/None at times.
                    row[col_name] = (
                        datetime(*(row[col_name].timetuple()[:6]))
                        .replace(tzinfo=tz.tzutc())
                        .isoformat()
                    )
        elif UUID_TYPE_RE.match(col_type):
            for row in iterate_rows():
                row[col_name] = str(row[col_name])

    return result
