from typing import Iterator

import pytest
from arroyo.backends.local.backend import LocalBroker
from arroyo.backends.local.storages.memory import MemoryMessageStorage
from arroyo.clock import TestingClock
from arroyo.metrics import configure_metrics
from arroyo.tests.metrics import TestingMetricsBackend
from arroyo.types import TPayload


def pytest_configure() -> None:
    configure_metrics(TestingMetricsBackend)


@pytest.fixture(autouse=True)
def clear_metrics_state() -> Iterator[None]:
    yield
    TestingMetricsBackend.calls.clear()


@pytest.fixture
def broker() -> Iterator[LocalBroker[TPayload]]:
    yield LocalBroker(MemoryMessageStorage(), TestingClock())
