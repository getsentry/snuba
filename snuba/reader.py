from __future__ import annotations

import re
from abc import ABC, abstractmethod
from datetime import datetime
from typing import TYPE_CHECKING, Any, Mapping, Optional, Sequence

from dateutil.tz import tz


if TYPE_CHECKING:
    from mypy_extensions import TypedDict

    Column = TypedDict("Column", {"name": str, "type": str})
    Result = TypedDict(
        "Result",
        {
            "meta": Sequence[Column],
            "data": Sequence[Mapping[str, Any]],
            "totals": Mapping[str, Any],
        },
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


class NativeDriverReader(Reader):

    DATE_TYPE_RE = re.compile(r"(Nullable\()?Date\b")
    DATETIME_TYPE_RE = re.compile(r"(Nullable\()?DateTime\b")

    def __init__(self, client):
        self.__client = client

    def __transform_result(self, result, with_totals: bool) -> Result:
        """
        Transform a native driver response into a response that is
        structurally similar to a ClickHouse-flavored JSON response.
        """
        data, meta = result

        data = [{c[0]: d[i] for i, c in enumerate(meta)} for d in result[0]]
        meta = [{"name": m[0], "type": m[1]} for m in result[1]]

        for col in meta:
            # Convert naive datetime strings back to TZ aware ones, and stringify
            # TODO maybe this should be in the json serializer
            if self.DATETIME_TYPE_RE.match(col["type"]):
                for d in data:
                    d[col["name"]] = (
                        d[col["name"]].replace(tzinfo=tz.tzutc()).isoformat()
                    )
            elif self.DATE_TYPE_RE.match(col["type"]):
                for d in data:
                    dt = datetime(*(d[col["name"]].timetuple()[:6])).replace(
                        tzinfo=tz.tzutc()
                    )
                    d[col["name"]] = dt.isoformat()

        if with_totals:
            assert len(data) > 0
            totals = data.pop(-1)
            return {"data": data, "meta": meta, "totals": totals}
        else:
            return {"data": data, "meta": meta}

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
