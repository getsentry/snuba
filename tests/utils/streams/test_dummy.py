import contextlib
from typing import Iterator
from unittest import TestCase

from snuba.utils.streams.dummy import (
    DummyBroker,
    DummyConsumer,
    DummyProducer,
)
from snuba.utils.streams.types import Topic
from tests.utils.streams.mixins import StreamsTestMixin


class DummyStreamsTestCase(StreamsTestMixin, TestCase):
    def setUp(self) -> None:
        self.broker: DummyBroker[int] = DummyBroker({Topic("test"): [[]]})

    @contextlib.contextmanager
    def get_topic(self) -> Iterator[Topic]:
        yield Topic("test")

    def get_consumer(self, group: str) -> DummyConsumer[int]:
        return DummyConsumer(self.broker, group, enable_end_of_partition=True)

    def get_producer(self) -> DummyProducer[int]:
        return DummyProducer(self.broker)
