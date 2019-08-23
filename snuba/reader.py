from __future__ import annotations

import itertools
import re
from abc import ABC, abstractmethod
from datetime import datetime
from typing import TYPE_CHECKING, Any, Mapping, MutableMapping, Optional, Sequence

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
                row[col["name"]] = (
                    row[col["name"]].replace(tzinfo=tz.tzutc()).isoformat()
                )
        elif DATE_TYPE_RE.match(col["type"]):
            for row in iterate_rows():
                row[col["name"]] = (
                    datetime(*(row[col["name"]].timetuple()[:6]))
                    .replace(tzinfo=tz.tzutc())
                    .isoformat()
                )

    return result


class NativeDriverReader(Reader):
    def __init__(self, client):
        self.__client = client

    def __transform_result(self, result, with_totals: bool) -> Result:
        """
        Transform a native driver response into a response that is
        structurally similar to a ClickHouse-flavored JSON response.
        """
        data, meta = result

        data = [{c[0]: d[i] for i, c in enumerate(meta)} for d in data]
        meta = [{"name": m[0], "type": m[1]} for m in meta]

        if with_totals:
            assert len(data) > 0
            totals = data.pop(-1)
            result = {"data": data, "meta": meta, "totals": totals}
        else:
            result = {"data": data, "meta": meta}

        return transform_date_columns(result)

    def execute(
        self,
        query: str,
        settings: Optional[Mapping[str, str]] = None,
        query_id: Optional[str] = None,
        with_totals: bool = False,
    ) -> Result:
        if settings is None:
            settings = {}

        kwargs = {}
        if query_id is not None:
            kwargs["query_id"] = query_id

        return self.__transform_result(
            self.__client.execute(
                query, with_column_types=True, settings=settings, **kwargs
            ),
            with_totals=with_totals,
        )
