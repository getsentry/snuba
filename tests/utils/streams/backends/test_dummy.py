from abc import abstractmethod
import contextlib
import itertools
import uuid
from typing import Iterator, Optional
from unittest import TestCase
from tempfile import TemporaryDirectory

import pytest

from snuba.utils.clock import TestingClock
from snuba.utils.streams.backends.dummy import (
    DummyBroker,
    DummyConsumer,
    DummyProducer,
    MessageStorage,
    MemoryMessageStorage,
)
from snuba.utils.streams.backends.file import FileMessageStorage
from snuba.utils.streams.types import Topic
from tests.utils.streams.backends.mixins import StreamsTestMixin


class DummyStreamsTestMixin(StreamsTestMixin[int]):
    def setUp(self) -> None:
        self.broker: DummyBroker[int] = DummyBroker(self.get_message_storage())

    @abstractmethod
    def get_message_storage(self) -> MessageStorage[int]:
        raise NotImplementedError

    @contextlib.contextmanager
    def get_topic(self, partitions: int = 1) -> Iterator[Topic]:
        topic = Topic(uuid.uuid1().hex)
        self.broker.create_topic(topic, partitions)
        yield topic

    def get_consumer(
        self, group: Optional[str] = None, enable_end_of_partition: bool = True
    ) -> DummyConsumer[int]:
        return self.broker.get_consumer(
            group if group is not None else uuid.uuid1().hex,
            enable_end_of_partition=enable_end_of_partition,
        )

    def get_producer(self) -> DummyProducer[int]:
        return self.broker.get_producer()

    def get_payloads(self) -> Iterator[int]:
        return itertools.count()

    @pytest.mark.xfail(strict=True, reason="rebalancing not implemented")
    def test_pause_resume_rebalancing(self) -> None:
        return super().test_pause_resume_rebalancing()


class DummyStreamsMemoryTestCase(DummyStreamsTestMixin, TestCase):
    def get_message_storage(self) -> MessageStorage[int]:
        return MemoryMessageStorage()


class DummyStreamsFileTestCase(DummyStreamsTestMixin, TestCase):
    def setUp(self) -> None:
        self.directory = TemporaryDirectory()
        super().setUp()

    def get_message_storage(self) -> MessageStorage[int]:
        return FileMessageStorage(self.directory.name, clock=TestingClock())
