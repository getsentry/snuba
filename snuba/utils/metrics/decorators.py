from functools import wraps
from typing import Callable, TypeVar

from snuba.utils.metrics.backends.abstract import MetricsBackend


T = TypeVar("T")


def track_calls(
    metrics: MetricsBackend, key: str, function: Callable[[], T]
) -> Callable[[], T]:
    """Tracks how frequently a function is called."""

    @wraps(function)
    def tracker() -> T:
        metrics.increment(key)
        return function()

    return tracker
