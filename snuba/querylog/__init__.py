from typing import Any, Mapping, Optional

import sentry_sdk
from sentry_sdk import Hub

from snuba import environment, settings, state
from snuba.querylog.query_metadata import QueryStatus, SnubaQueryMetadata
from snuba.request import Request
from snuba.utils.metrics.timer import Timer
from snuba.utils.metrics.wrapper import MetricsWrapper

metrics = MetricsWrapper(environment.metrics, "api")


def record_query(
    request: Request,
    timer: Timer,
    query_metadata: SnubaQueryMetadata,
    extra_data: Mapping[str, Any],
) -> None:
    """
    Records a request after it has been parsed and validated, whether
    we actually ran a query or not.
    """
    if settings.RECORD_QUERIES:
        # Send to redis
        # We convert this to a dict before passing it to state in order to avoid a
        # circular dependency, where state would depend on the higher level
        # QueryMetadata class
        state.record_query(query_metadata.to_dict())

        final = str(request.query.get_final())
        referrer = request.referrer or "none"
        timer.send_metrics_to(
            metrics,
            tags={
                "status": query_metadata.status.value,
                "referrer": referrer,
                "parent_api": request.settings.get_parent_api(),
                "final": final,
            },
            mark_tags={
                "final": final,
                "referrer": referrer,
                "parent_api": request.settings.get_parent_api(),
            },
        )

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


def record_invalid_request(timer: Timer, referrer: Optional[str]) -> None:
    """
    Records a failed request before the request object is created, so
    it records failures during parsing/validation.
    This is for client errors.
    """
    _record_failure_building_request(QueryStatus.INVALID_REQUEST, timer, referrer)


def record_error_building_request(timer: Timer, referrer: Optional[str]) -> None:
    """
    Records a failed request before the request object is created, so
    it records failures during parsing/validation.
    This is for system errors during parsing/validation.
    """
    _record_failure_building_request(QueryStatus.ERROR, timer, referrer)


def _record_failure_building_request(
    status: QueryStatus, timer: Timer, referrer: Optional[str]
) -> None:
    # TODO: Revisit if recording some data for these queries in the querylog
    # table would be useful.
    if settings.RECORD_QUERIES:
        timer.send_metrics_to(
            metrics, tags={"status": status.value, "referrer": referrer or "none"},
        )
        _add_tags(timer)
