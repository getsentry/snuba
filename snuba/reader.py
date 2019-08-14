from __future__ import annotations

import itertools
import re
from abc import ABC, abstractmethod
from datetime import datetime
from typing import TYPE_CHECKING, Any, Mapping, MutableMapping, Optional, Sequence
from urllib.parse import urlencode, urljoin

import requests
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


class HTTPReader(Reader):
    def __init__(
        self, host: str, port: int, settings: Optional[Mapping[str, str]] = None
    ):
        if settings is not None:
            assert "query_id" not in settings, "query_id cannot be passed as a setting"
        self.__base_url = f"http://{host}:{port}/"
        self.__default_settings = settings if settings is not None else {}

    def execute(
        self,
        query: str,
        settings: Optional[Mapping[str, str]] = None,
        query_id: Optional[str] = None,
        with_totals: bool = False,  # NOTE: unnecessary with FORMAT JSON
    ) -> Result:
        if settings is None:
            settings = {}

        assert "query_id" not in settings, "query_id cannot be passed as a setting"
        if query_id is not None:
            settings["query_id"] = query_id

        response = requests.post(
            urljoin(
                self.__base_url,
                "?" + urlencode({**self.__default_settings, **settings}),
            ),
            data=query.encode("utf-8"),
        )

        # TODO: Distinguish between ClickHouse errors and other HTTP errors.
        if response.status_code != 200:
            raise Exception(response.content)

        result = response.json()
        del result["statistics"]
        del result["rows"]

        return transform_date_columns(result)
