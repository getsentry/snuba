from __future__ import annotations

from concurrent.futures import Future, wait
from threading import Barrier, Thread
from typing import Any, Callable

import pytest

from snuba.utils.metrics.backends.abstract import MetricsBackend
from snuba.utils.metrics.gauge import Gauge, ThreadSafeGauge
from snuba.utils.metrics.types import Tags
from tests.backends.metrics import Gauge as GaugeCall
from tests.backends.metrics import TestingMetricsBackend


def execute(function: Callable[[], Any]) -> Future[Any]:
    future: Future[Any] = Future()

    def run() -> None:
        future.set_result(function())

    Thread(target=run).start()

    return future


@pytest.mark.parametrize("factory", [Gauge, ThreadSafeGauge])
def test_gauge_simple(factory: Callable[[MetricsBackend, str, Tags], Gauge]) -> None:
    backend = TestingMetricsBackend()

    name = "name"
    tags = {"tag": "value"}
    gauge = factory(backend, name, tags)

    with gauge:
        pass

    assert backend.calls == [
        GaugeCall(name, 0.0, tags),
        GaugeCall(name, 1.0, tags),
        GaugeCall(name, 0.0, tags),
    ]


def test_gauge_concurrent() -> None:
    backend = TestingMetricsBackend()

    name = "name"
    tags = {"tag": "value"}
    gauge = ThreadSafeGauge(backend, name, tags)

    workers = 4
    barrier = Barrier(workers)

    def waiter() -> None:
        with gauge:
            barrier.wait()

    wait([execute(waiter) for i in range(workers)])

    assert backend.calls == [
        GaugeCall(name, 0.0, tags),
        GaugeCall(name, 1.0, tags),
        GaugeCall(name, 2.0, tags),
        GaugeCall(name, 3.0, tags),
        GaugeCall(name, 4.0, tags),
        GaugeCall(name, 3.0, tags),
        GaugeCall(name, 2.0, tags),
        GaugeCall(name, 1.0, tags),
        GaugeCall(name, 0.0, tags),
    ]
