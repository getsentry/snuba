import contextlib
import itertools
import uuid
from abc import abstractmethod
from datetime import datetime
from tempfile import TemporaryDirectory
from typing import Iterator, Optional
from unittest import TestCase

import pytest

from snuba.utils.clock import TestingClock
from snuba.utils.streams.backends.abstract import Consumer, Producer
from snuba.utils.streams.backends.local.backend import LocalBroker
from snuba.utils.streams.backends.local.storages.abstract import (
    MessageStorage,
    PartitionDoesNotExist,
    TopicDoesNotExist,
    TopicExists,
)
from snuba.utils.streams.backends.local.storages.file import (
    FileMessageStorage,
    InvalidChecksum,
)
from snuba.utils.streams.backends.local.storages.memory import MemoryMessageStorage
from snuba.utils.streams.types import Partition, Topic
from tests.utils.streams.backends.mixins import StreamsTestMixin


class LocalStreamsTestMixin(StreamsTestMixin[int]):
    def setUp(self) -> None:
        self.storage = self.get_message_storage()
        self.broker = LocalBroker(self.storage, TestingClock())

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

    def test_storage(self) -> None:
        topic = Topic(uuid.uuid1().hex)
        partitions = 3

        self.storage.create_topic(topic, partitions)

        assert [*self.storage.list_topics()] == [topic]

        assert self.storage.get_partition_count(topic) == partitions

        with pytest.raises(TopicExists):
            self.storage.create_topic(topic, partitions)

        with pytest.raises(TopicDoesNotExist):
            self.storage.get_partition_count(Topic("invalid"))

        with pytest.raises(TopicDoesNotExist):
            self.storage.consume(Partition(Topic("invalid"), 0), 0)

        with pytest.raises(TopicDoesNotExist):
            self.storage.produce(Partition(Topic("invalid"), 0), 0, datetime.now())

        with pytest.raises(PartitionDoesNotExist):
            self.storage.consume(Partition(topic, -1), 0)

        with pytest.raises(PartitionDoesNotExist):
            self.storage.consume(Partition(topic, partitions + 1), 0)

        with pytest.raises(PartitionDoesNotExist):
            self.storage.produce(Partition(topic, -1), 0, datetime.now())

        with pytest.raises(PartitionDoesNotExist):
            self.storage.produce(Partition(topic, partitions + 1), 0, datetime.now())

        self.storage.delete_topic(topic)

        with pytest.raises(TopicDoesNotExist):
            self.storage.delete_topic(topic)


class LocalStreamsMemoryStorageTestCase(LocalStreamsTestMixin, TestCase):
    def get_message_storage(self) -> MessageStorage[int]:
        return MemoryMessageStorage()


class LocalStreamsFileStorageTestCase(LocalStreamsTestMixin, TestCase):
    def setUp(self) -> None:
        self.directory = TemporaryDirectory()
        super().setUp()

    def get_message_storage(self) -> MessageStorage[int]:
        return FileMessageStorage(self.directory.name)

    def test_unaligned_offset(self) -> None:
        topic = Topic(uuid.uuid1().hex)
        partition = Partition(topic, 0)
        self.storage.create_topic(topic, 1)

        message = self.storage.produce(partition, 1, datetime.now())

        invalid_offset = message.offset + 4
        assert message.next_offset > invalid_offset > message.offset

        with pytest.raises(InvalidChecksum):
            self.storage.consume(partition, invalid_offset)
