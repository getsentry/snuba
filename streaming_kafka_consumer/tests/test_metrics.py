from streaming_kafka_consumer.metrics import Gauge
from streaming_kafka_consumer.tests.metrics import Gauge as GaugeCall
from streaming_kafka_consumer.tests.metrics import TestingMetricsBackend


def test_gauge_simple() -> None:
    backend = TestingMetricsBackend()

    name = "name"
    tags = {"tag": "value"}
    gauge = Gauge(backend, name, tags)

    with gauge:
        pass

    assert backend.calls == [
        GaugeCall(name, 0.0, tags),
        GaugeCall(name, 1.0, tags),
        GaugeCall(name, 0.0, tags),
    ]
