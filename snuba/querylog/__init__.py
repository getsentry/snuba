from snuba.request import Request
from snuba.utils.metrics.timer import Timer
from snuba import environment, settings, state
from snuba.utils.metrics.backends.wrapper import MetricsWrapper
from snuba.querylog.query_metadata import SnubaQueryMetadata

metrics = MetricsWrapper(environment.metrics, "api")


def record_query(
    request: Request, timer: Timer, query_metadata: SnubaQueryMetadata
) -> None:
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
