from dataclasses import dataclass
from enum import Enum
from typing import Any, Mapping, MutableSequence, Optional

from snuba.request import Request
from snuba.utils.metrics.timer import Timer


class QueryStatus(Enum):
    SUCCESS = "success"
    ERROR = "error"  # A system error
    RATE_LIMITED = "rate-limited"
    INVALID_REQUEST = "invalid-request"


@dataclass(frozen=True)
class ClickhouseQueryMetadata:
    sql: str
    stats: Mapping[str, Any]
    status: QueryStatus
    trace_id: Optional[str] = None

    def to_dict(self) -> Mapping[str, Any]:
        return {
            "sql": self.sql,
            "stats": self.stats,
            "status": self.status.value,
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

    def to_dict(self) -> Mapping[str, Any]:
        return {
            "request": {
                "id": self.request.id,
                "body": self.request.body,
                "referrer": self.request.referrer,
            },
            "dataset": self.dataset,
            "query_list": [q.to_dict() for q in self.query_list],
            "status": self.status.value,
            "timing": self.timer.for_json(),
        }

    @property
    def status(self) -> QueryStatus:
        # If we do not have any recorded query and we did not specifically log
        # invalid_query, assume there was an error somewhere.
        return self.query_list[-1].status if self.query_list else QueryStatus.ERROR
