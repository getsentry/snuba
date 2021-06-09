from typing import Iterator

import pytest
from streaming_kafka_consumer.backends.local.backend import LocalBroker
from streaming_kafka_consumer.backends.local.storages.memory import MemoryMessageStorage
from streaming_kafka_consumer.clock import TestingClock
from streaming_kafka_consumer.metrics import configure_metrics
from streaming_kafka_consumer.tests.metrics import TestingMetricsBackend
from streaming_kafka_consumer.types import TPayload


def pytest_configure() -> None:
    configure_metrics(TestingMetricsBackend)


@pytest.fixture(autouse=True)
def clear_metrics_state() -> Iterator[None]:
    yield
    TestingMetricsBackend.calls.clear()


@pytest.fixture
def broker() -> Iterator[LocalBroker[TPayload]]:
    yield LocalBroker(MemoryMessageStorage(), TestingClock())
