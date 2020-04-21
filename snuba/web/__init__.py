from typing import Any, Mapping, NamedTuple

from snuba.reader import Result


class RawQueryException(Exception):
    """
    Exception raiesd during query execution that is used to carry context
    back up the stack to the HTTP response. This exception should always be
    chained with another exception that contains additional detail about the
    cause of the exception.
    """

    def __init__(self, stats: Mapping[str, Any], sql: str):
        self.stats = stats
        self.sql = sql


class RawQueryResult(NamedTuple):
    result: Result
    extra: Any
