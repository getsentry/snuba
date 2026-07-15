import _strptime  # NOQA fixes _strptime deferred import issue
import inspect
import threading
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
    """Create a DogStatsd object if DOGSTATSD_HOST and DOGSTATSD_PORT are defined.

    When the ``use_dogstatsd_uds`` sentry-option is ``True`` and ``DOGSTATSD_SOCKET_PATH``
    is configured, metrics are sent over the Unix domain socket instead of UDP; with the
    flag off (or no socket configured) they use UDP (host/port). The flag is authoritative
    -- it never falls back to the socket when off -- so host/port stay configured as the
    UDP transport/rollback target. Return a DummyMetricsBackend when no host/port is set.
    Prefixes must start with `snuba.<category>`, for example: `snuba.processor`.
    """
    host: str | None = settings.DOGSTATSD_HOST
    port: int | None = settings.DOGSTATSD_PORT
    socket_path: str | None = settings.DOGSTATSD_SOCKET_PATH

    if settings.TESTING:
        from snuba.utils.metrics.backends.testing import TestingMetricsBackend

        return TestingMetricsBackend()

    if host is None and port is None:
        from snuba.utils.metrics.backends.dummy import DummyMetricsBackend

        return DummyMetricsBackend()

    if host is None or port is None:
        raise ValueError(
            f"DOGSTATSD_HOST and DOGSTATSD_PORT should both be None or not None. Found DOGSTATSD_HOST: {host}, DOGSTATSD_PORT: {port} instead."
        )

    from datadog import DogStatsd  # type: ignore[attr-defined]  # datadog lacks explicit re-export

    from snuba.utils.metrics.backends.datadog import DatadogMetricsBackend
    from snuba.utils.metrics.backends.dualwrite import SentryDatadogMetricsBackend
    from snuba.utils.metrics.backends.sentry import SentryMetricsBackend

    constant_tags = [f"{key}:{value}" for key, value in tags.items()] if tags is not None else None
    udp = (host, port)

    # Resolve UDS-vs-UDP once for the whole process. DatadogMetricsBackend builds a
    # DogStatsd client per thread, so the factory below runs on each thread's first
    # metric emission; caching the decision keeps every thread on the same transport
    # instead of letting a mid-process option flip put some threads on UDP and others on
    # UDS. The value is fixed until restart, matching the documented "flip the option,
    # then restart" contract. Double-checked locking: the lock is only contended on the
    # first emission per thread until the decision is cached.
    resolved_use_uds: bool | None = None
    resolve_lock = threading.Lock()

    def make_client() -> DogStatsd:
        nonlocal resolved_use_uds
        if resolved_use_uds is None:
            # The use_dogstatsd_uds sentry-option is read lazily here -- on the first
            # metric emission -- not at create_metrics() time. create_metrics() runs while
            # snuba.environment is being imported, and importing snuba.state.sentry_options
            # pulls in the snuba.state package, whose __init__ binds
            # MetricsWrapper(environment.metrics, ...) at import time, so importing it any
            # earlier would be a circular import. By first-emit time sentry-options has been
            # initialized (snuba.environment.setup_sentry -> init_options); if it hasn't,
            # get_option returns the False default and we stay on UDP.
            from snuba.state.sentry_options import get_option

            with resolve_lock:
                if resolved_use_uds is None:
                    resolved_use_uds = socket_path is not None and bool(
                        get_option("use_dogstatsd_uds", False)
                    )

        if resolved_use_uds:
            return DogStatsd(
                socket_path=socket_path,
                namespace=prefix,
                constant_tags=constant_tags,
            )
        return DogStatsd(
            host=udp[0],
            port=udp[1],
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
