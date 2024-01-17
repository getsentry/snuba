from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any, Dict, MutableSequence, Optional, Set, cast

from clickhouse_driver.errors import ErrorCodes
from sentry_kafka_schemas.schema_types import snuba_queries_v1

from snuba.clickhouse.errors import ClickhouseError
from snuba.datasets.storage import StorageNotAvailable
from snuba.query.exceptions import InvalidQueryException
from snuba.request import Request
from snuba.request.exceptions import InvalidJsonRequestException
from snuba.state.cache.abstract import ExecutionTimeoutError
from snuba.state.rate_limit import TABLE_RATE_LIMIT_NAME, RateLimitExceeded
from snuba.utils.metrics.timer import Timer
from snuba.web import QueryTooLongException


class QueryStatus(Enum):
    SUCCESS = "success"
    ERROR = "error"  # A system error
    RATE_LIMITED = "rate-limited"
    INVALID_REQUEST = "invalid-request"
    TIMEOUT = "timeout"


CLICKHOUSE_ERROR_TO_SNUBA_ERROR_MAPPINGS = {
    ErrorCodes.TOO_SLOW: QueryStatus.TIMEOUT,
    ErrorCodes.TIMEOUT_EXCEEDED: QueryStatus.TIMEOUT,
    ErrorCodes.SOCKET_TIMEOUT: QueryStatus.TIMEOUT,
    ErrorCodes.NETWORK_ERROR: QueryStatus.TIMEOUT,
}


def get_query_status_from_error_codes(code: ErrorCodes) -> QueryStatus | None:
    return CLICKHOUSE_ERROR_TO_SNUBA_ERROR_MAPPINGS.get(code)


class RequestStatus(Enum):
    """
    The different statuses we return for a request.

    TODO: This will replace QueryStatus, but both exist as we cut over.
    """

    # Successfully returned a response
    SUCCESS = "success"
    # Failed validation
    INVALID_REQUEST = "invalid-request"
    # A type mismatch between arguments, ILLEGAL_TYPE_OF_ARGUMENT/TYPE_MISMATCH
    INVALID_TYPING = "invalid-typing"
    # Rejected because of one of the product based rate limiters
    RATE_LIMITED = "rate-limited"
    # Rejected by a table rate limiter
    TABLE_RATE_LIMITED = "table-rate-limited"
    # If the thread responsible for executing a query and writing it to the cache times out
    CACHE_SET_TIMEOUT = "cache-set-timeout"
    # The thread waiting for a cache result to be written times out
    CACHE_WAIT_TIMEOUT = "cache-wait-timeout"
    # If the query takes longer than 30 seconds to run, and the thread is killed by the cache
    QUERY_TIMEOUT = "query-timeout"
    # Clickhouse predicted the query would time out. TOO_SLOW
    PREDICTED_TIMEOUT = "predicted-timeout"
    # Clickhouse timeout, TIMEOUT_EXCEEDED
    CLICKHOUSE_TIMEOUT = "clickhouse-timeout"
    # Network isssues, SOCKET_TIMEOUT NETWORK_ERROR
    NETWORK_TIMEOUT = "network-timeout"
    # Query used too much memory, MEMORY_LIMIT_EXCEEDED
    MEMORY_EXCEEDED = "memory-exceeded"
    # Any other error
    ERROR = "error"


class SLO(Enum):
    FOR = "for"  # These are not counted against our SLO
    AGAINST = "against"  # These are counted against our SLO


@dataclass(frozen=True)
class Status:
    status: RequestStatus

    @property
    def slo(self) -> SLO:
        return SLO.FOR if self.status in SLO_FOR else SLO.AGAINST


# Statuses that don't impact the SLO
SLO_FOR = {
    RequestStatus.SUCCESS,
    RequestStatus.INVALID_REQUEST,
    RequestStatus.INVALID_TYPING,
    RequestStatus.RATE_LIMITED,
    RequestStatus.QUERY_TIMEOUT,
    RequestStatus.PREDICTED_TIMEOUT,
    RequestStatus.MEMORY_EXCEEDED,
}


ERROR_CODE_MAPPINGS = {
    ErrorCodes.TOO_SLOW: RequestStatus.PREDICTED_TIMEOUT,
    ErrorCodes.TIMEOUT_EXCEEDED: RequestStatus.CLICKHOUSE_TIMEOUT,
    ErrorCodes.SOCKET_TIMEOUT: RequestStatus.NETWORK_TIMEOUT,
    ErrorCodes.NETWORK_ERROR: RequestStatus.NETWORK_TIMEOUT,
    ErrorCodes.ILLEGAL_TYPE_OF_ARGUMENT: RequestStatus.INVALID_TYPING,
    ErrorCodes.TYPE_MISMATCH: RequestStatus.INVALID_TYPING,
    ErrorCodes.NO_COMMON_TYPE: RequestStatus.INVALID_TYPING,
    ErrorCodes.ILLEGAL_COLUMN: RequestStatus.INVALID_REQUEST,
    ErrorCodes.UNKNOWN_FUNCTION: RequestStatus.INVALID_REQUEST,
    ErrorCodes.MEMORY_LIMIT_EXCEEDED: RequestStatus.MEMORY_EXCEEDED,
    ErrorCodes.CANNOT_PARSE_UUID: RequestStatus.INVALID_REQUEST,
    ErrorCodes.ILLEGAL_AGGREGATION: RequestStatus.INVALID_REQUEST,
}


def get_request_status(cause: Exception | None = None) -> Status:
    slo_status: RequestStatus
    if cause is None:
        slo_status = RequestStatus.SUCCESS
    elif isinstance(cause, RateLimitExceeded):
        trigger_rate_limiter = cause.extra_data.get("scope", "")
        if trigger_rate_limiter == TABLE_RATE_LIMIT_NAME:
            slo_status = RequestStatus.TABLE_RATE_LIMITED
        else:
            slo_status = RequestStatus.RATE_LIMITED
    elif isinstance(cause, ClickhouseError):
        slo_status = ERROR_CODE_MAPPINGS.get(cause.code, RequestStatus.ERROR)
    elif isinstance(cause, TimeoutError):
        slo_status = RequestStatus.QUERY_TIMEOUT
    elif isinstance(cause, ExecutionTimeoutError):
        slo_status = RequestStatus.CACHE_WAIT_TIMEOUT
    elif isinstance(
        cause, (StorageNotAvailable, InvalidJsonRequestException, InvalidQueryException)
    ):
        slo_status = RequestStatus.INVALID_REQUEST
    elif isinstance(cause, QueryTooLongException):
        slo_status = RequestStatus.INVALID_REQUEST
    else:
        slo_status = RequestStatus.ERROR

    return Status(slo_status)


Columnset = Set[str]


@dataclass(frozen=True)
class FilterProfile:
    # Lists all the columns in the filter
    columns: Columnset
    # Filters on non optimized mapping columns like tags/contexts
    mapping_cols: Columnset

    def to_dict(self) -> snuba_queries_v1.ClickhouseQueryProfileWhereProfile:
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

    def to_dict(self) -> snuba_queries_v1.ClickhouseQueryProfile:
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
    sql_anonymized: str
    start_timestamp: Optional[datetime]
    end_timestamp: Optional[datetime]
    stats: Dict[str, Any]
    status: QueryStatus
    request_status: Status
    profile: ClickhouseQueryProfile
    trace_id: Optional[str] = None
    result_profile: Optional[Dict[str, Any]] = None

    def to_dict(self) -> snuba_queries_v1.QueryMetadata:
        start = int(self.start_timestamp.timestamp()) if self.start_timestamp else None
        end = int(self.end_timestamp.timestamp()) if self.end_timestamp else None
        return {
            "sql": self.sql,
            "sql_anonymized": self.sql_anonymized,
            "start_timestamp": start,
            "end_timestamp": end,
            "stats": cast(snuba_queries_v1._QueryMetadataStats, self.stats),
            "status": self.status.value,
            "request_status": self.request_status.status.value,
            "slo": self.request_status.slo.value,
            "trace_id": self.trace_id,
            "profile": self.profile.to_dict(),
            "result_profile": self.result_profile,
        }


@dataclass(frozen=True)
class SnubaQueryMetadata:
    """
    Metadata about a Snuba query for recording on the querylog dataset.
    """

    request: Request
    start_timestamp: Optional[datetime]
    end_timestamp: Optional[datetime]
    dataset: str
    entity: str
    timer: Timer
    query_list: MutableSequence[ClickhouseQueryMetadata]
    projects: Set[int]
    snql_anonymized: str

    def to_dict(self) -> snuba_queries_v1.Querylog:
        start = int(self.start_timestamp.timestamp()) if self.start_timestamp else None
        end = int(self.end_timestamp.timestamp()) if self.end_timestamp else None
        request_dict: snuba_queries_v1.Querylog = {
            "request": {
                "id": self.request.id,
                "body": self.request.original_body,
                "referrer": self.request.referrer,
                "team": self.request.attribution_info.team,
                "feature": self.request.attribution_info.feature,
                "app_id": self.request.attribution_info.app_id.key,
            },
            "dataset": self.dataset,
            "entity": self.entity,
            "start_timestamp": start,
            "end_timestamp": end,
            "query_list": [q.to_dict() for q in self.query_list],
            "status": self.status.value,
            "request_status": self.request_status.value,
            "slo": self.slo.value,
            "timing": self.timer.for_json(),
            "projects": list(self.projects),
            "snql_anonymized": self.snql_anonymized,
        }
        # TODO: Remove check once Org IDs are required
        org_id = self.request.attribution_info.tenant_ids.get("organization_id")
        if org_id is not None and isinstance(org_id, int):
            request_dict["organization"] = org_id
        return request_dict

    @property
    def status(self) -> QueryStatus:
        # If we do not have any recorded query and we did not specifically log
        # invalid_query, we assume there was an error somewhere.
        return self.query_list[-1].status if self.query_list else QueryStatus.ERROR

    @property
    def request_status(self) -> RequestStatus:
        # If we do not have any recorded query and we did not specifically log
        # invalid_query, we assume there was an error somewhere.
        if not self.query_list:
            return RequestStatus.ERROR

        for query in self.query_list:
            if query.request_status.status != RequestStatus.SUCCESS:
                return query.request_status.status  # always return the worst case

        return RequestStatus.SUCCESS

    @property
    def slo(self) -> SLO:
        # If we do not have any recorded query and we did not specifically log
        # invalid_query, we assume there was an error and log it against.
        if not self.query_list:
            return SLO.AGAINST

        # even one error counts the request against
        failure = any(q.request_status.slo != SLO.FOR for q in self.query_list)
        return SLO.AGAINST if failure else SLO.FOR
