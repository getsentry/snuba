from snuba.utils.metrics.backends.testing import (
    Gauge,
    Increment,
    TestingMetricsBackend,
    Timing,
)
from snuba.utils.metrics.metrics import Metrics


def test_increment() -> None:
    backend = TestingMetricsBackend()
    metrics = Metrics(backend)

    metrics.increment("key", 1)
    assert backend.calls[-1] == Increment("key", 1, {})

    metrics.increment("key", 1, {"key": "value"})
    assert backend.calls[-1] == Increment("key", 1, {"key": "value"})


def test_gauge() -> None:
    backend = TestingMetricsBackend()
    metrics = Metrics(backend)

    metrics.gauge("key", 1)
    assert backend.calls[-1] == Gauge("key", 1, {})

    metrics.gauge("key", 1, {"key": "value"})
    assert backend.calls[-1] == Gauge("key", 1, {"key": "value"})


def test_timing() -> None:
    backend = TestingMetricsBackend()
    metrics = Metrics(backend)

    metrics.timing("key", 1)
    assert backend.calls[-1] == Timing("key", 1, {})

    metrics.timing("key", 1, {"key": "value"})
    assert backend.calls[-1] == Timing("key", 1, {"key": "value"})


def test_constant_tags() -> None:
    backend = TestingMetricsBackend()
    metrics = Metrics(backend, tags={"constant-key": "value"})

    # TODO: More comprehensive per-method coverage?

    metrics.increment("key", 1)
    assert backend.calls[-1] == Increment("key", 1, {"constant-key": "value"})

    metrics.increment("key", 1, tags={"instance-key": "value"})
    assert backend.calls[-1] == Increment(
        "key", 1, {"constant-key": "value", "instance-key": "value"}
    )

    metrics.increment("key", 1, tags={"constant-key": "override"})
    assert backend.calls[-1] == Increment("key", 1, {"constant-key": "override"})


def test_namespace() -> None:
    backend = TestingMetricsBackend()
    metrics = Metrics(backend, namespace="prefix")

    # TODO: More comprehensive per-method coveragye?

    metrics.increment("key", 1)
    assert backend.calls[-1] == Increment("prefix.key", 1, {})


def test_sampling_rate() -> None:
    backend = TestingMetricsBackend()
    metrics = Metrics(backend, sample_rates={"sampled": 0.0})

    # TODO: More comprehensive per-method coverage?

    metrics.increment("sampled", 1)
    assert backend.calls == []

    metrics.increment("unsampled", 1, {})
    assert backend.calls == [Increment("unsampled", 1, {})]
