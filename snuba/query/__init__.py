from typing import Any, NamedTuple, Optional

from snuba.reader import Result


class RawQueryResult(NamedTuple):
    result: Result
    applied_sampling_rate: Optional[float]
    extra: Any
