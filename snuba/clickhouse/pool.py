from __future__ import annotations

import re
from abc import ABC, abstractmethod
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import (
    Any,
    TypedDict,
    cast,
)
from uuid import UUID

from dateutil.tz import tz

from snuba.clickhouse.formatter.nodes import FormattedQuery
from snuba.reader import Reader, Result, build_result_transformer

Params = Sequence[Any] | dict[str, Any] | None


class ClickhouseProfile(TypedDict):
    bytes: int
    progress_bytes: int
    blocks: int
    rows: int
    elapsed: float


@dataclass(frozen=True)
class ClickhouseResult:
    results: Sequence[Any] = field(default_factory=list)
    meta: Sequence[Any] | None = None
    profile: ClickhouseProfile | None = None
    trace_output: str = ""
    query_id: str = ""


class ClickhousePool(ABC):
    """
    Abstract base for a pool of ClickHouse connections.

    The concrete implementation is ``ClickhouseConnectPool``
    (snuba.clickhouse.connect), which talks HTTP via clickhouse-connect.
    Callers receive connections typed as ``ClickhousePool`` and only rely on the
    methods/attributes declared here.
    """

    host: str
    port: int
    user: str
    password: str
    database: str

    @abstractmethod
    def execute(
        self,
        query: str,
        params: Params = None,
        with_column_types: bool = False,
        query_id: str | None = None,
        settings: Mapping[str, Any] | None = None,
        types_check: bool = False,
        columnar: bool = False,
        capture_trace: bool = False,
    ) -> ClickhouseResult:
        raise NotImplementedError

    @abstractmethod
    def execute_robust(
        self,
        query: str,
        params: Params = None,
        with_column_types: bool = False,
        query_id: str | None = None,
        settings: Mapping[str, Any] | None = None,
        types_check: bool = False,
        columnar: bool = False,
        capture_trace: bool = False,
    ) -> ClickhouseResult:
        raise NotImplementedError

    def execute_explain(self, query: str) -> ClickhouseResult:
        """
        Run an EXPLAIN statement and return its single ``explain`` text column,
        one row per line.

        The clickhouse-connect pool overrides this because its Native result
        reader cannot decode EXPLAIN responses through :meth:`execute`. Callers
        that issue EXPLAIN (snuba-admin system-query validation) must use this
        method rather than ``execute``.
        """
        return self.execute(query, with_column_types=True)

    def execute_with_totals(
        self,
        query: str,
        params: Params = None,
        query_id: str | None = None,
        settings: Mapping[str, Any] | None = None,
        capture_trace: bool = False,
        robust: bool = False,
    ) -> ClickhouseResult:
        """
        Run a ``WITH TOTALS`` query, returning the totals as the trailing result row
        (the shape :class:`ClickhouseReader` expects). The clickhouse-connect pool
        overrides this because its normal output drops the totals block. Callers
        must use this rather than ``execute``.
        """
        execute = self.execute_robust if robust else self.execute
        return execute(
            query,
            params=params,
            with_column_types=True,
            query_id=query_id,
            settings=settings,
            capture_trace=capture_trace,
        )

    def insert(
        self,
        table: str,
        data: Sequence[Mapping[str, Any]],
        settings: Mapping[str, Any] | None = None,
        query_id: str | None = None,
    ) -> None:
        rows = list(data)
        if not rows:
            return
        self.execute(
            f"INSERT INTO {table} FORMAT JSONEachRow",
            rows,
            settings=settings,
            query_id=query_id,
        )

    @abstractmethod
    def close(self) -> None:
        raise NotImplementedError


def transform_date(value: date) -> str:
    """
    Convert a timezone-naive date object into an ISO 8601 formatted date and
    time string respresentation.
    """
    # XXX: Both Python and ClickHouse date objects do not have time zones, so
    # just assume UTC. (Ideally, we'd have just left these as timezone naive to
    # begin with and not done this transformation at all, since the time
    # portion has no benefit or significance here.)
    return datetime(*value.timetuple()[:6]).replace(tzinfo=tz.tzutc()).isoformat()


def transform_datetime(value: datetime) -> str:
    """
    Convert a timezone-naive datetime object into an ISO 8601 formatted date
    and time string representation.
    """
    if value.tzinfo is None:
        value = value.replace(tzinfo=tz.tzutc())
    else:
        value = value.astimezone(tz.tzutc())
    return value.isoformat()


def transform_uuid(value: UUID) -> str:
    """
    Convert a UUID object into a string representation.
    """
    return str(value)


transform_column_types = build_result_transformer(
    [
        (re.compile(r"^Date(\(.+\))?$"), transform_date),
        (re.compile(r"^DateTime(\(.+\))?$"), transform_datetime),
        (re.compile(r"^UUID$"), transform_uuid),
    ]
)


class ClickhouseReader(Reader):
    """
    Reader for ClickHouse queries. It adapts a :class:`ClickhouseResult` into the
    JSON-flavored ``Result``. It wraps the abstract :class:`ClickhousePool`.
    """

    def __init__(
        self,
        cache_partition_id: str | None,
        client: ClickhousePool,
        query_settings_prefix: str | None,
    ) -> None:
        super().__init__(
            cache_partition_id=cache_partition_id,
            query_settings_prefix=query_settings_prefix,
        )
        self.__client = client

    def __transform_result(self, result: ClickhouseResult, with_totals: bool) -> Result:
        """
        Transform a driver response into a response that is structurally similar
        to a ClickHouse-flavored JSON response.
        """
        meta = result.meta if result.meta is not None else []
        data = result.results
        profile = cast(dict[str, Any] | None, result.profile)
        # XXX: Rows are represented as mappings that are keyed by column or
        # alias, which is problematic when the result set contains duplicate
        # names. To ensure that the column headers and row data are consistent
        # duplicated names are discarded at this stage.
        columns = {c[0]: i for i, c in enumerate(meta)}

        # Build the column-keyed row dicts by overwriting the source list in
        # place rather than via a second list comprehension, so each row tuple is
        # freed as its dict replacement is created instead of keeping the full
        # tuple list and the full dict list alive at once.
        if not isinstance(data, list):
            data = list(data)
        column_items = list(columns.items())
        for i, row in enumerate(data):
            data[i] = {column: row[index] for column, index in column_items}

        meta = [{"name": m[0], "type": m[1]} for m in [meta[i] for i in columns.values()]]

        new_result: Result = {}
        if with_totals:
            # The pool returns the totals as the trailing row (see
            # ClickhousePool.execute_with_totals); an empty result means it went missing.
            assert len(data) > 0, "WITH TOTALS query returned no rows (missing totals row)"
            totals = data.pop(-1)
            new_result = {
                "data": data,
                "meta": meta,
                "totals": totals,
                "profile": profile,
                "trace_output": result.trace_output,
            }
        else:
            new_result = {
                "data": data,
                "meta": meta,
                "profile": profile,
                "trace_output": result.trace_output,
            }

        transform_column_types(new_result)

        return new_result

    def execute(
        self,
        query: FormattedQuery,
        # TODO: move Clickhouse specific arguments into clickhouse.query.Query
        settings: Mapping[str, str] | None = None,
        with_totals: bool = False,
        robust: bool = False,
        capture_trace: bool = False,
    ) -> Result:
        settings = {**settings} if settings is not None else {}

        query_id = None
        if "query_id" in settings:
            query_id = settings.pop("query_id")

        if with_totals:
            # Totals travel through the pool entry point (see
            # ClickhousePool.execute_with_totals).
            result = self.__client.execute_with_totals(
                query.get_sql(),
                query_id=query_id,
                settings=settings,
                capture_trace=capture_trace,
                robust=robust,
            )
        else:
            execute_func = self.__client.execute_robust if robust else self.__client.execute
            result = execute_func(
                query.get_sql(),
                with_column_types=True,
                query_id=query_id,
                settings=settings,
                capture_trace=capture_trace,
            )

        return self.__transform_result(result, with_totals=with_totals)
