import _strptime  # NOQA fixes _strptime deferred import issue
import inspect
from functools import wraps
from typing import Any, TypeVar, cast
from collections.abc import Callable, Mapping

import sentry_sdk

from snuba import settings
from snuba.utils.metrics import MetricsBackend
from snuba.utils.metrics.types import Tags


def create_metrics(
    prefix: str,
    tags: Tags | None = None,
    sample_rates: Mapping[str, float] | None = None,
) -> MetricsBackend:
    """Create a DogStatsd object if a DogStatsD Unix domain socket is configured.

    Metrics are sent to the local DogStatsD agent over a Unix domain socket;
    ``DOGSTATSD_SOCKET_PATH`` is the only supported transport. Return a
    DummyMetricsBackend when it is not configured.
    ``DOGSTATSD_SOCKET_PATH`` is a full address including the transport scheme, e.g.
    ``unixgram:///run/dogstatsd.sock``; it is passed to the datadog client verbatim (the
    client strips the scheme), the same value the Rust exporter consumes.
    Prefixes must start with `snuba.<category>`, for example: `snuba.processor`.
    """
    socket_path: str | None = settings.DOGSTATSD_SOCKET_PATH

    if settings.TESTING:
        from snuba.utils.metrics.backends.testing import TestingMetricsBackend

        return TestingMetricsBackend()

    # No socket configured -> no metrics.
    if socket_path is None:
        from snuba.utils.metrics.backends.dummy import DummyMetricsBackend

        return DummyMetricsBackend()

    from datadog import DogStatsd  # type: ignore[attr-defined]  # datadog lacks explicit re-export

    from snuba.utils.metrics.backends.datadog import DatadogMetricsBackend
    from snuba.utils.metrics.backends.dualwrite import SentryDatadogMetricsBackend
    from snuba.utils.metrics.backends.sentry import SentryMetricsBackend

    constant_tags = [f"{key}:{value}" for key, value in tags.items()] if tags is not None else None

    def make_client() -> DogStatsd:
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
        )

    return SentryDatadogMetricsBackend(
        DatadogMetricsBackend(make_client, sample_rates),
        SentryMetricsBackend(),
    )


F = TypeVar("F", bound=Callable[..., Any])


def with_span(op: str = "function") -> Callable[[F], F]:
    """Wraps a function call in a Sentry AM span"""

    def decorator(func: F) -> F:
        frame_info = inspect.stack()[1]
        filename = frame_info.filename

        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            with sentry_sdk.start_span(description=func.__name__, op=op) as span:
                span.set_data("filename", filename)
                return func(*args, **kwargs)

        return cast(F, wrapper)

    return decorator
