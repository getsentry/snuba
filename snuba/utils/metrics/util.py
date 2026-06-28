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
    """Create a DogStatsd object if DOGSTATSD_HOST and DOGSTATSD_PORT are defined,
    or if a Unix domain socket is configured via ``DOGSTATSD_SOCKET_PATH``. Return a
    DummyMetricsBackend otherwise.
    Prefixes must start with `snuba.<category>`, for example: `snuba.processor`.
    """
    host: str | None = settings.DOGSTATSD_HOST
    port: int | None = settings.DOGSTATSD_PORT
    socket_path: str | None = settings.DOGSTATSD_SOCKET_PATH

    if settings.TESTING:
        from snuba.utils.metrics.backends.testing import TestingMetricsBackend

        return TestingMetricsBackend()

    if host is None and port is None and socket_path is None:
        from snuba.utils.metrics.backends.dummy import DummyMetricsBackend

        return DummyMetricsBackend()

    if (host is None) != (port is None):
        raise ValueError(
            f"DOGSTATSD_HOST and DOGSTATSD_PORT should both be None or not None. Found DOGSTATSD_HOST: {host}, DOGSTATSD_PORT: {port} instead."
        )

    from datadog import DogStatsd  # type: ignore[attr-defined]  # datadog lacks explicit re-export

    from snuba.utils.metrics.backends.datadog import DatadogMetricsBackend
    from snuba.utils.metrics.backends.dualwrite import SentryDatadogMetricsBackend
    from snuba.utils.metrics.backends.sentry import SentryMetricsBackend

    constant_tags = [f"{key}:{value}" for key, value in tags.items()] if tags is not None else None
    udp = (host, port) if host is not None and port is not None else None

    def make_client() -> DogStatsd:
        # The use_dogstatsd_uds flag is read lazily here -- when the first metric is
        # emitted -- not at create_metrics() time. create_metrics() runs while
        # snuba.environment is being imported, and snuba.state binds
        # MetricsWrapper(environment.metrics, ...) at its own import time, so importing
        # snuba.state any earlier would be a circular import.
        from snuba import state

        use_uds = socket_path is not None and str(state.get_config("use_dogstatsd_uds", "0")) == "1"

        # Use UDP when it is configured and UDS is not enabled; otherwise (UDS enabled,
        # or only a socket configured) use the Unix domain socket.
        if udp is not None and not use_uds:
            return DogStatsd(
                host=udp[0],
                port=udp[1],
                namespace=prefix,
                constant_tags=constant_tags,
            )
        return DogStatsd(
            socket_path=socket_path,
            namespace=prefix,
            constant_tags=constant_tags,
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
