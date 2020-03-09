from typing import Any, NamedTuple

from snuba.reader import Result


class RawQueryResult(NamedTuple):
    result: Result
    extra: Any
