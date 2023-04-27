from __future__ import annotations

from typing import Any, Mapping, NamedTuple, TypedDict, cast

from snuba.reader import Column, Result, Row, transform_rows
from snuba.utils.serializable_exception import JsonSerializable, SerializableException


class QueryExtraData(TypedDict):
    stats: Mapping[str, Any]
    sql: str
    experiments: Mapping[str, Any]


class QueryException(SerializableException):
    """
    Exception raised during query execution that is used to carry extra data
    back up the stack to the HTTP response -- basically a ``QueryResult``,
    but without an actual ``Result`` instance. This exception should always
    be chained with another exception that contains additional detail about
    the cause of the exception.
    """

    def __init__(
        self,
        exception_type: str | None = None,
        message: str | None = None,
        should_report: bool = True,
        **extra_data: JsonSerializable,
    ) -> None:
        self.exception_type = exception_type
        super().__init__(message, should_report, **extra_data)

    @classmethod
    def from_args(
        cls, exception_type: str, message: str, extra: QueryExtraData
    ) -> "QueryException":

        return cls(
            exception_type=exception_type,
            message=message,
            extra=cast(JsonSerializable, extra),
        )

    @property
    def extra(self) -> QueryExtraData:
        extra = self.extra_data.get("extra", None)
        if not extra:
            return QueryExtraData(stats={}, sql="noquery", experiments={})
        return cast(QueryExtraData, extra)


class QueryTooLongException(SerializableException):
    """
    Exception thrown when a query string is too long for ClickHouse.

    There is a limit for the maximum size of a query (in bytes)
    ClickHouse will process, this limit is defined in Snuba settings.
    """


class QueryResult(NamedTuple):
    result: Result
    extra: QueryExtraData


def transform_column_names(
    result: QueryResult, mapping: Mapping[str, list[str]]
) -> None:
    """
    Replaces the column names in a ResultSet object in place.
    """

    def transformer(row: Row) -> Row:
        new_row: Row = {}
        for key, value in row.items():
            column_names = mapping.get(key, [key])
            for c in column_names:
                new_row[c] = value
        return new_row

    transform_rows(result.result, transformer)

    new_meta = []
    for c in result.result["meta"]:
        names = mapping.get(c["name"], [c["name"]])
        for n in names:
            new_meta.append(Column(name=n, type=c["type"]))
    result.result["meta"] = new_meta
