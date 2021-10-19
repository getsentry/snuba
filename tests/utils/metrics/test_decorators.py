from unittest.mock import sentinel

from snuba.utils.metrics.decorators import track_calls
from tests.backends.metrics import Increment, TestingMetricsBackend


def test_track_calls() -> None:
    backend = TestingMetricsBackend()
    key = "key"
    assert (
        track_calls(backend, key, lambda: sentinel.RETURN_VALUE)()
        is sentinel.RETURN_VALUE
    )
    assert backend.calls == [Increment(key, 1, None)]
