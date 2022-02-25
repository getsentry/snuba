from __future__ import annotations

from typing import Any, Mapping, NamedTuple

from mypy_extensions import TypedDict

from snuba.reader import Column, Result, Row, transform_rows


class QueryExtraData(TypedDict):
    stats: Mapping[str, Any]
    sql: str
    experiments: Mapping[str, Any]


class QueryException(Exception):
    """
    Exception raised during query execution that is used to carry extra data
    back up the stack to the HTTP response -- basically a ``QueryResult``,
    but without an actual ``Result`` instance. This exception should always
    be chained with another exception that contains additional detail about
    the cause of the exception.
    """

    def __init__(self, extra: QueryExtraData):
        self.extra = extra

    def __str__(self) -> str:
        return f"{self.__cause__} {super().__str__()}"


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
