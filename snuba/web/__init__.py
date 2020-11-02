from typing import Any, Mapping, NamedTuple

from mypy_extensions import TypedDict

from snuba.reader import Result, transform_rows, Column


class QueryExtraData(TypedDict):
    stats: Mapping[str, Any]
    sql: str


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


class QueryResult(NamedTuple):
    result: Result
    extra: QueryExtraData


def transform_column_names(result: QueryResult, mapping: Mapping[str, str]) -> None:
    """
    Replaces the column names in a ResultSet object in place.
    """

    transform_rows(
        result.result,
        lambda row: {mapping.get(key, key): value for key, value in row.items()},
    )

    result.result["meta"] = [
        Column(name=mapping.get(c["name"], c["name"]), type=c["type"])
        for c in result.result["meta"]
    ]
