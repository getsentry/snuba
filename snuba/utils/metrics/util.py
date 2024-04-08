import inspect
from functools import partial, wraps
from typing import Any, Callable, Mapping, Optional, TypeVar, cast

import _strptime  # NOQA fixes _strptime deferred import issue
import sentry_sdk

from snuba import settings
from snuba.utils.metrics import MetricsBackend
from snuba.utils.metrics.types import Tags


def create_metrics(
    prefix: str,
    tags: Optional[Tags] = None,
    sample_rates: Optional[Mapping[str, float]] = None,
) -> MetricsBackend:
    """Create a DogStatsd object if DOGSTATSD_HOST and DOGSTATSD_PORT are defined,
    with the specified prefix and tags. Return a DummyMetricsBackend otherwise.
    Prefixes must start with `snuba.<category>`, for example: `snuba.processor`.
    """
    host: Optional[str] = settings.DOGSTATSD_HOST
    port: Optional[int] = settings.DOGSTATSD_PORT

    if settings.TESTING:
        from snuba.utils.metrics.backends.testing import TestingMetricsBackend

        return TestingMetricsBackend()
    elif host is None and port is None:
        from snuba.utils.metrics.backends.dummy import DummyMetricsBackend

        return DummyMetricsBackend()
    elif host is None or port is None:
        raise ValueError(
            f"DOGSTATSD_HOST and DOGSTATSD_PORT should both be None or not None. Found DOGSTATSD_HOST: {host}, DOGSTATSD_PORT: {port} instead."
        )

    from datadog import DogStatsd

    from snuba.utils.metrics.backends.datadog import DatadogMetricsBackend
    from snuba.utils.metrics.backends.dualwrite import SentryDatadogMetricsBackend
    from snuba.utils.metrics.backends.sentry import SentryMetricsBackend

    return SentryDatadogMetricsBackend(
        DatadogMetricsBackend(
            partial(
                DogStatsd,
                host=host,
                port=port,
                namespace=prefix,
                constant_tags=[f"{key}:{value}" for key, value in tags.items()]
                if tags is not None
                else None,
            ),
            sample_rates,
        ),
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
