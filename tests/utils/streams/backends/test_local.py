import contextlib
import itertools
import uuid
from abc import abstractmethod
from tempfile import TemporaryDirectory
from typing import Iterator, Optional
from unittest import TestCase

import pytest

from snuba.utils.clock import TestingClock
from snuba.utils.streams.backends.abstract import Consumer, Producer
from snuba.utils.streams.backends.local.storages.abstract import MessageStorage
from snuba.utils.streams.backends.local.storages.file import FileMessageStorage
from snuba.utils.streams.backends.local.storages.memory import MemoryMessageStorage
from snuba.utils.streams.backends.local.backend import LocalBroker
from snuba.utils.streams.types import Topic
from tests.utils.streams.backends.mixins import StreamsTestMixin


class LocalStreamsTestMixin(StreamsTestMixin[int]):
    def setUp(self) -> None:
        self.broker: LocalBroker[int] = LocalBroker(
            self.get_message_storage(), TestingClock()
        )

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
    ) -> Consumer[int]:
        return self.broker.get_consumer(
            group if group is not None else uuid.uuid1().hex,
            enable_end_of_partition=enable_end_of_partition,
        )

    def get_producer(self) -> Producer[int]:
        return self.broker.get_producer()

    def get_payloads(self) -> Iterator[int]:
        return itertools.count()

    @pytest.mark.xfail(strict=True, reason="rebalancing not implemented")
    def test_pause_resume_rebalancing(self) -> None:
        return super().test_pause_resume_rebalancing()


class LocalStreamsMemoryStorageTestCase(LocalStreamsTestMixin, TestCase):
    def get_message_storage(self) -> MessageStorage[int]:
        return MemoryMessageStorage()


class LocalStreamsFileStorageTestCase(LocalStreamsTestMixin, TestCase):
    def setUp(self) -> None:
        self.directory = TemporaryDirectory()
        super().setUp()

    def get_message_storage(self) -> MessageStorage[int]:
        return FileMessageStorage(self.directory.name)
