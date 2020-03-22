from typing import Any, Mapping, NamedTuple

from snuba.reader import Result


class RawQueryException(Exception):
    def __init__(
        self, err_type: str, message: str, stats: Mapping[str, Any], sql: str, **meta
    ):
        self.err_type = err_type
        self.message = message
        self.stats = stats
        self.sql = sql
        self.meta = meta


class RawQueryResult(NamedTuple):
    result: Result
    extra: Any
