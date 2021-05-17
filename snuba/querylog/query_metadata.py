from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, Mapping, MutableSequence, Optional, Set

from snuba.request import Request
from snuba.utils.metrics.timer import Timer


class QueryStatus(Enum):
    SUCCESS = "success"
    ERROR = "error"  # A system error
    RATE_LIMITED = "rate-limited"
    INVALID_REQUEST = "invalid-request"


Columnset = Set[str]


@dataclass(frozen=True)
class FilterProfile:
    # Lists all the columns in the filter
    columns: Columnset
    # Filters on non optimized mapping columns like tags/contexts
    mapping_cols: Columnset

    def to_dict(self) -> Mapping[str, Any]:
        return {
            "columns": sorted(self.columns),
            "mapping_cols": sorted(self.mapping_cols),
        }


@dataclass(frozen=True)
class ClickhouseQueryProfile:
    """
    Summarizes some profiling information from the query ast to make
    it easier to analyze both in the querylog and in discover.
    """

    time_range: Optional[int]  # range in days
    table: str
    all_columns: Columnset
    # True if we have a combination of AND and OR instead of
    # only having AND conditions.
    multi_level_condition: bool
    # Columns in the where clause
    where_profile: FilterProfile
    # Group by clause
    groupby_cols: Columnset
    # Columns in arrayjoin statements
    array_join_cols: Columnset

    def to_dict(self) -> Mapping[str, Any]:
        return {
            "time_range": self.time_range,
            "table": self.table,
            "all_columns": sorted(self.all_columns),
            "multi_level_condition": self.multi_level_condition,
            "where_profile": self.where_profile.to_dict(),
            "groupby_cols": sorted(self.groupby_cols),
            "array_join_cols": sorted(self.array_join_cols),
        }


@dataclass(frozen=True)
class ClickhouseQueryMetadata:
    sql: str
    stats: Mapping[str, Any]
    status: QueryStatus
    profile: ClickhouseQueryProfile
    trace_id: Optional[str] = None

    def to_dict(self) -> Mapping[str, Any]:
        return {
            "sql": self.sql,
            "stats": self.stats,
            "status": self.status.value,
            "trace_id": self.trace_id,
            "profile": self.profile.to_dict(),
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

    def to_dict(self) -> Dict[str, Any]:
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
        # invalid_query, we assume there was an error somewhere.
        return self.query_list[-1].status if self.query_list else QueryStatus.ERROR
