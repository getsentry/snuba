from concurrent.futures import wait
from threading import Barrier
from snuba.utils.concurrent import execute
from snuba.utils.metrics.gauge import Gauge
from tests.backends.metrics import TestingMetricsBackend, Gauge as GaugeCall


def test_gauge_simple() -> None:
    backend = TestingMetricsBackend()

    name = "name"
    tags = {"tag": "value"}
    gauge = Gauge(backend, name, tags)

    with gauge:
        pass

    assert backend.calls == [
        GaugeCall(name, 0, tags),
        GaugeCall(name, 1, tags),
        GaugeCall(name, 0, tags),
    ]


def test_gauge_concurrent() -> None:
    backend = TestingMetricsBackend()

    name = "name"
    tags = {"tag": "value"}
    gauge = Gauge(backend, name, tags)

    workers = 4
    barrier = Barrier(workers)

    def waiter() -> None:
        with gauge:
            barrier.wait()

    wait([execute(waiter) for i in range(workers)])

    assert backend.calls == [
        GaugeCall(name, 0, tags),
        GaugeCall(name, 1, tags),
        GaugeCall(name, 2, tags),
        GaugeCall(name, 3, tags),
        GaugeCall(name, 4, tags),
        GaugeCall(name, 3, tags),
        GaugeCall(name, 2, tags),
        GaugeCall(name, 1, tags),
        GaugeCall(name, 0, tags),
    ]
