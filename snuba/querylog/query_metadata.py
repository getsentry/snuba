from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any, Dict, MutableSequence, Optional, Set, cast

from clickhouse_driver.errors import ErrorCodes
from sentry_kafka_schemas.schema_types import snuba_queries_v1

from snuba.clickhouse.errors import ClickhouseError
from snuba.clickhouse.query_dsl.accessors import get_time_range
from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.storage import StorageNotAvailable
from snuba.query.data_source.projects_finder import ProjectsFinder
from snuba.query.exceptions import InvalidQueryException
from snuba.query.logical import Query as LogicalQuery
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
    # Clickhouse exceeded max_concurrent_queries limit
    CLICKHOUSE_MAX_QUERIES_EXCEEDED = "clickhouse-max-queries-exceeded"
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
    ErrorCodes.TOO_MANY_SIMULTANEOUS_QUERIES: RequestStatus.CLICKHOUSE_MAX_QUERIES_EXCEEDED,
    ErrorCodes.CANNOT_PARSE_DOMAIN_VALUE_FROM_STRING: RequestStatus.INVALID_REQUEST,
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
    trace_id: str
    result_profile: Optional[snuba_queries_v1._QueryMetadataResultProfileObject] = None

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


class SnubaQueryMetadata:
    """
    Metadata about a Snuba query for recording on the querylog dataset.
    """

    def __init__(
        self,
        request: Request,
        dataset: str,
        timer: Timer,
        start_timestamp: datetime | None = None,
        end_timestamp: datetime | None = None,
        entity: str | None = None,
        query_list: MutableSequence[ClickhouseQueryMetadata] | None = None,
        projects: Set[int] | None = None,
        snql_anonymized: str | None = None,
    ):
        if not (
            (start_timestamp is None)
            == (end_timestamp is None)
            == (entity is None)
            == (query_list is None)
            == (projects is None)
            == (snql_anonymized is None)
        ):
            raise ValueError("Must provide all or none of the optional parameters")

        self.request = request
        self.dataset = dataset
        self.timer = timer

        if start_timestamp is None:
            start, end = None, None
            entity_name = "unknown"
            if isinstance(request.query, LogicalQuery):
                source_key = request.query.get_from_clause().key
                if isinstance(source_key, EntityKey):
                    entity_key = source_key
                    entity_obj = get_entity(entity_key)
                    entity_name = entity_key.value
                    if entity_obj.required_time_column is not None:
                        start, end = get_time_range(
                            request.query, entity_obj.required_time_column
                        )
            self.start_timestamp = start
            self.end_timestamp = end
            self.entity = entity_name
            self.query_list: MutableSequence[ClickhouseQueryMetadata] = []
            self.projects = ProjectsFinder().visit(request.query)
            self.snql_anonymized = ""
        else:
            self.start_timestamp = start_timestamp
            self.end_timestamp = end_timestamp
            assert (
                entity is not None
                and query_list is not None
                and projects is not None
                and snql_anonymized is not None
            )  # mypy sucks
            self.entity = entity
            self.query_list = query_list
            self.projects = projects
            self.snql_anonymized = snql_anonymized

    def to_dict(self) -> snuba_queries_v1.Querylog:
        start = int(self.start_timestamp.timestamp()) if self.start_timestamp else None
        end = int(self.end_timestamp.timestamp()) if self.end_timestamp else None
        request_dict: snuba_queries_v1.Querylog = {
            "request": {
                "id": self.request.id.hex,
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
