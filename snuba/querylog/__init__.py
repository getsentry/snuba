from typing import Optional

from snuba import environment, settings, state
from snuba.querylog.query_metadata import QueryStatus, SnubaQueryMetadata
from snuba.request import Request
from snuba.utils.metrics.timer import Timer
from snuba.utils.metrics.wrapper import MetricsWrapper

metrics = MetricsWrapper(environment.metrics, "api")


def record_query(
    request: Request, timer: Timer, query_metadata: SnubaQueryMetadata
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
                "final": final,
            },
            mark_tags={"final": final},
        )


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
