from typing import Any, Mapping, Optional, Union

import sentry_sdk
from sentry_sdk import Hub

from snuba import environment, settings, state
from snuba.attribution.log import (
    AttributionData,
    QueryAttributionData,
    record_attribution,
)
from snuba.datasets.storage import StorageNotAvailable
from snuba.query.exceptions import QueryPlanException
from snuba.querylog.query_metadata import QueryStatus, SnubaQueryMetadata, Status
from snuba.request import Request
from snuba.utils.metrics.timer import Timer
from snuba.utils.metrics.wrapper import MetricsWrapper
from snuba.web import QueryException, QueryResult

metrics = MetricsWrapper(environment.metrics, "api")
from snuba.querylog.query_metadata import get_request_status


def _record_timer_metrics(
    request: Request,
    timer: Timer,
    query_metadata: SnubaQueryMetadata,
    result: Union[QueryResult, QueryException, QueryPlanException],
) -> None:
    final = str(request.query.get_final())
    referrer = request.referrer or "none"
    app_id = request.attribution_info.app_id.key or "none"
    parent_api = request.attribution_info.parent_api or "none"
    tags = {
        "status": query_metadata.status.value,
        "request_status": query_metadata.request_status.value,
        "slo": query_metadata.slo.value,
        "referrer": referrer,
        "parent_api": parent_api,
        "final": final,
        "dataset": query_metadata.dataset,
        "app_id": app_id,
    }
    mark_tags = {
        "final": final,
        "referrer": referrer,
        "parent_api": parent_api,
        "dataset": query_metadata.dataset,
    }
    if isinstance(result, StorageNotAvailable):
        # The QueryPlanException is raised outside the query execution flow.
        # As a result, its status and SLO values are not based on its query_list
        status = get_request_status(result)
        tags = {
            "status": QueryStatus.ERROR.value,
            "request_status": status.status.value,
            "slo": status.slo.value,
            "referrer": referrer,
            "parent_api": parent_api,
            "final": final,
            "dataset": query_metadata.dataset,
            "app_id": app_id,
        }

    timer.send_metrics_to(
        metrics,
        tags=tags,
        mark_tags=mark_tags,
    )


def _record_attribution_metrics(
    request: Request, query_metadata: SnubaQueryMetadata, extra_data: Mapping[str, Any]
) -> None:
    timing_data = query_metadata.timer.for_json()
    attr_data = AttributionData(
        app_id=request.attribution_info.app_id,
        referrer=request.referrer,
        parent_api=request.attribution_info.parent_api or "none",
        request_id=request.id,
        dataset=query_metadata.dataset,
        entity=query_metadata.entity,
        timestamp=timing_data["timestamp"],
        duration_ms=timing_data["duration_ms"],
        queries=[],
    )
    query_id = ""
    if "stats" in extra_data and "query_id" in extra_data["stats"]:
        query_id = extra_data["stats"]["query_id"]

    for q in query_metadata.query_list:
        profile = q.result_profile
        bytes_scanned = profile.get("bytes", 0.0) if profile else 0.0
        attr_query = QueryAttributionData(
            table=q.profile.table,
            query_id=query_id,
            bytes_scanned=bytes_scanned,
        )
        attr_data.queries.append(attr_query)

    record_attribution(attr_data)


def record_query(
    request: Request,
    timer: Timer,
    query_metadata: SnubaQueryMetadata,
    result: Union[QueryResult, QueryException, QueryPlanException],
) -> None:
    """
    Records a request after it has been parsed and validated, whether
    we actually ran a query or not.
    """

    extra_data: Mapping[str, Any] = {}
    if not isinstance(result, QueryPlanException):
        extra_data = result.extra
    if settings.RECORD_QUERIES:
        # Send to redis
        # We convert this to a dict before passing it to state in order to avoid a
        # circular dependency, where state would depend on the higher level
        # QueryMetadata class
        state.record_query(query_metadata.to_dict())
        _record_timer_metrics(request, timer, query_metadata, result)
        _record_attribution_metrics(request, query_metadata, extra_data)
        _add_tags(timer, extra_data.get("experiments"), query_metadata)


def _add_tags(
    timer: Timer,
    experiments: Optional[Mapping[str, Any]] = None,
    metadata: Optional[SnubaQueryMetadata] = None,
) -> None:
    if Hub.current.scope.span:
        duration_group = timer.get_duration_group()
        sentry_sdk.set_tag("duration_group", duration_group)
        if duration_group == ">30s":
            sentry_sdk.set_tag("timeout", "too_long")
        if experiments is not None:
            for name, value in experiments.items():
                sentry_sdk.set_tag(name, str(value))
        if metadata is not None:
            for query_data in metadata.query_list:
                max_threads = query_data.stats.get("max_threads")
                if max_threads is not None:
                    sentry_sdk.set_tag("max_threads", max_threads)
                    break


def record_invalid_request(
    timer: Timer, request_status: Status, referrer: Optional[str]
) -> None:
    """
    Records a failed request before the request object is created, so
    it records failures during parsing/validation.
    This is for client errors.
    """
    _record_failure_building_request(
        QueryStatus.INVALID_REQUEST, request_status, timer, referrer
    )


def record_error_building_request(
    timer: Timer, request_status: Status, referrer: Optional[str]
) -> None:
    """
    Records a failed request before the request object is created, so
    it records failures during parsing/validation.
    This is for system errors during parsing/validation.
    """
    _record_failure_building_request(QueryStatus.ERROR, request_status, timer, referrer)


def _record_failure_building_request(
    status: QueryStatus,
    request_status: Status,
    timer: Timer,
    referrer: Optional[str],
) -> None:
    # TODO: Revisit if recording some data for these queries in the querylog
    # table would be useful.
    if settings.RECORD_QUERIES:
        timer.send_metrics_to(
            metrics,
            tags={
                "status": status.value,
                "referrer": referrer or "none",
                "request_status": request_status.status.value,
                "slo": request_status.slo.value,
            },
        )
        _add_tags(timer)
