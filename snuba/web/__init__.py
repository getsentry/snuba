from typing import Any, Mapping, NamedTuple

from mypy_extensions import TypedDict

from snuba.reader import Result


class RawQueryExtraData(TypedDict):
    stats: Mapping[str, Any]
    sql: str


class RawQueryException(Exception):
    """
    Exception raised during query execution that is used to carry extra data
    back up the stack to the HTTP response -- basically a ``RawQueryResult``,
    but without an actual ``Result`` instance. This exception should always
    be chained with another exception that contains additional detail about
    the cause of the exception.
    """

    def __init__(self, extra: RawQueryExtraData):
        self.extra = extra


class RawQueryResult(NamedTuple):
    result: Result
    extra: RawQueryExtraData
