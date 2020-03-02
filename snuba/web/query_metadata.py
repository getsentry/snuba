from dataclasses import dataclass
from typing import Any, Mapping, MutableSequence, Optional

from snuba.request import Request
from snuba.utils.metrics.timer import Timer


@dataclass(frozen=True)
class ClickhouseQueryMetadata:
    sql: str
    stats: Mapping[str, Any]
    status: str
    trace_id: Optional[str] = None

    def to_dict(self) -> Mapping[str, Any]:
        return {
            "sql": self.sql,
            "stats": self.stats,
            "status": self.status,
            "trace_id": self.trace_id,
        }


@dataclass(frozen=True)
class SnubaQueryMetadata:
    """
    Metadata about a Snuba query for recording on the querylog dataset.
    """

    request: Request
    dataset: str
    timer: Timer
    query_list: MutableSequence[ClickhouseQueryMetadata]
    referrer: Optional[str] = None

    def to_dict(self) -> Mapping[str, Any]:
        return {
            "referrer": self.referrer,
            "dataset": self.dataset,
            "query_list": [q.to_dict() for q in self.query_list],
            "request": self.request.body,
            "status": self.status,
            "timing": self.timer.for_json(),
        }

    @property
    def status(self) -> str:
        return self.query_list[-1].status if self.query_list else "error"
