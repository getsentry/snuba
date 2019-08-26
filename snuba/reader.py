from __future__ import annotations

import itertools
import re
from abc import ABC, abstractmethod
from datetime import datetime
from typing import TYPE_CHECKING, Any, Mapping, MutableMapping, Optional, Sequence

from dateutil.parser import parse as dateutil_parse
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


class Reader(ABC):
    @abstractmethod
    def execute(
        self,
        query: str,
        settings: Optional[Mapping[str, str]] = None,
        query_id: Optional[str] = None,
        with_totals: bool = False,
    ) -> Result:
        """Execute a query."""
        raise NotImplementedError


DATE_TYPE_RE = re.compile(r"(Nullable\()?Date\b")
DATETIME_TYPE_RE = re.compile(r"(Nullable\()?DateTime\b")


def transform_date_columns(result: Result) -> Result:
    """
    Converts timezone-naive date and datetime values returned by ClickHouse
    into ISO 8601 formatted strings (including the UTC offset.)
    """

    def iterate_rows():
        if "totals" in result:
            return itertools.chain(result["data"], [result["totals"]])
        else:
            return iter(result["data"])

    for col in result["meta"]:
        if DATETIME_TYPE_RE.match(col["type"]):
            for row in iterate_rows():
                value = row[col["name"]]
                if isinstance(value, str):
                    value = dateutil_parse(value)
                row[col["name"]] = value.replace(tzinfo=tz.tzutc()).isoformat()
        elif DATE_TYPE_RE.match(col["type"]):
            for row in iterate_rows():
                value = row[col["name"]]
                if isinstance(value, str):
                    value = dateutil_parse(value)
                row[col["name"]] = (
                    datetime(*(value.timetuple()[:6]))
                    .replace(tzinfo=tz.tzutc())
                    .isoformat()
                )

    return result
