from __future__ import annotations

import _strptime  # NOQA fixes _strptime deferred import issue
import atexit
import inspect
import threading
from collections.abc import Callable, Mapping
from functools import wraps
from typing import Any, TypeVar, cast

from sentry_sdk import traces

from snuba import settings
from snuba.utils.metrics import MetricsBackend
from snuba.utils.metrics.types import Tags
from snuba.utils.sentry import SENTRY_OP

# Matches the Rust consumers' metrics-exporter-dogstatsd default flush interval.
AGGREGATION_FLUSH_INTERVAL_SECONDS = 3.0


def create_metrics(
    prefix: str,
    tags: Tags | None = None,
    sample_rates: Mapping[str, float] | None = None,
) -> MetricsBackend:
    """Create a DogStatsd object if a DogStatsD Unix domain socket is configured.

    Metrics are sent to the local DogStatsD agent over the socket configured by
    ``settings.DOGSTATSD_SOCKET_PATH``, which is the only supported transport and is passed
    to the datadog client verbatim. Return a DummyMetricsBackend when it is not configured.
    The ``dogstatsd_client_aggregation`` sentry-option switches from one unbuffered client
    per thread to one buffered, aggregating client shared by the whole process.
    Prefixes must start with `snuba.<category>`, for example: `snuba.processor`.
    """
    if settings.TESTING:
        from snuba.utils.metrics.backends.testing import TestingMetricsBackend

        return TestingMetricsBackend()

    # No socket configured -> no metrics.
    socket_path: str | None = settings.DOGSTATSD_SOCKET_PATH
    if socket_path is None:
        from snuba.utils.metrics.backends.dummy import DummyMetricsBackend

        return DummyMetricsBackend()

    from datadog import DogStatsd  # type: ignore[attr-defined]  # datadog lacks explicit re-export

    from snuba.utils.metrics.backends.datadog import DatadogMetricsBackend
    from snuba.utils.metrics.backends.dualwrite import SentryDatadogMetricsBackend
    from snuba.utils.metrics.backends.sentry import SentryMetricsBackend

    constant_tags = [f"{key}:{value}" for key, value in tags.items()] if tags is not None else None

    # With dogstatsd_client_aggregation on, every thread gets the same client, so counts
    # and gauges aggregate across threads into one flush (like the Rust consumers' single
    # global recorder) instead of each thread writing every call to the agent socket.
    shared_client: DogStatsd | None = None
    resolved = False
    resolve_lock = threading.Lock()

    def new_client(**aggregation: Any) -> DogStatsd:
        # socket_path is passed to the datadog client verbatim. It is expected to be a
        # full address including the transport scheme (e.g.
        # "unixgram:///run/dogstatsd.sock"); the datadog client strips the scheme and
        # selects the socket kind itself. The same env var (SNUBA_DOGSTATSD_SOCKET_PATH)
        # is passed verbatim to the Rust exporter, which parses the scheme too, so no
        # scheme is hardcoded on either side.
        return DogStatsd(
            socket_path=socket_path,
            namespace=prefix,
            constant_tags=constant_tags,
            disable_telemetry=False,
            **aggregation,
        )

    def make_client() -> DogStatsd:
        nonlocal shared_client, resolved
        if not resolved:
            # The options are read lazily here -- on the first metric emission -- not at
            # create_metrics() time. create_metrics() runs while snuba.environment is being
            # imported, and importing snuba.state.sentry_options pulls in the snuba.state
            # package, whose __init__ binds MetricsWrapper(environment.metrics, ...) at
            # import time, so importing it any earlier would be a circular import. If
            # sentry-options isn't initialized yet, get_option returns the defaults.
            from snuba.state.sentry_options import get_option

            with resolve_lock:
                if not resolved:
                    if get_option("dogstatsd_client_aggregation", False):
                        shared_client = new_client(
                            disable_buffering=False,
                            disable_aggregation=False,
                            flush_interval=AGGREGATION_FLUSH_INTERVAL_SECONDS,
                            max_metric_samples_per_context=get_option(
                                "dogstatsd_max_samples_per_context", 0
                            ),
                        )
                        # The client flushes from a daemon thread, so flush the last
                        # window on exit instead of dropping it.
                        atexit.register(shared_client.stop)
                    resolved = True

        if shared_client is not None:
            return shared_client
        return new_client()

    return SentryDatadogMetricsBackend(
        DatadogMetricsBackend(make_client, sample_rates),
        SentryMetricsBackend(),
    )


F = TypeVar("F", bound=Callable[..., Any])


def with_span(op: str = "function") -> Callable[[F], F]:
    """Wraps a function call in a Sentry AM span"""

    def decorator(func: F) -> F:
        frame_info = inspect.stack()[1]
        attributes: dict[str, Any] = {SENTRY_OP: op, "filename": frame_info.filename}

        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            with traces.start_span(name=func.__name__, attributes=attributes):
                return func(*args, **kwargs)

        return cast(F, wrapper)

    return decorator


def set_current_span_attributes(attributes: Mapping[str, Any]) -> None:
    """Set attributes on the active stream-mode span, if any.

    Replaces ``sentry_sdk.update_current_span()``, a no-op under stream mode.
    """
    span = traces.get_current_span()
    if span is None:
        return
    span.set_attributes(dict(attributes))
