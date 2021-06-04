from typing import Iterator

import pytest
from streaming_kafka_consumer.backends.local.backend import LocalBroker
from streaming_kafka_consumer.backends.local.storages.memory import MemoryMessageStorage
from streaming_kafka_consumer.clock import TestingClock
from streaming_kafka_consumer.types import TPayload


@pytest.fixture
def broker() -> Iterator[LocalBroker[TPayload]]:
    yield LocalBroker(MemoryMessageStorage(), TestingClock())
