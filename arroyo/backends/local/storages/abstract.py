from abc import ABC, abstractmethod
from datetime import datetime
from typing import Generic, Iterator, Optional

from arroyo.types import Message, Partition, Topic, TPayload


class TopicExists(Exception):
    def __init__(self, topic: Topic) -> None:
        self.topic = topic


class TopicDoesNotExist(Exception):
    def __init__(self, topic: Topic) -> None:
        self.topic = topic


class PartitionDoesNotExist(Exception):
    def __init__(self, partition: Partition) -> None:
        self.partition = partition


class MessageStorage(ABC, Generic[TPayload]):
    @abstractmethod
    def create_topic(self, topic: Topic, partitions: int) -> None:
        """
        Create a topic with the given number of partitions.

        If the topic already exists, a ``TopicExists`` exception will be
        raised.
        """
        raise NotImplementedError

    @abstractmethod
    def list_topics(self) -> Iterator[Topic]:
        """
        List all topics.
        """
        raise NotImplementedError

    @abstractmethod
    def delete_topic(self, topic: Topic) -> None:
        """
        Delete a topic.

        If the topic does not exist, a ``TopicDoesNotExist`` exception will
        be raised.
        """
        raise NotImplementedError

    @abstractmethod
    def get_partition_count(self, topic: Topic) -> int:
        """
        Get the number of partitions within a topic.

        If the topic does not exist, a ``TopicDoesNotExist`` exception will
        be raised.
        """
        raise NotImplementedError

    @abstractmethod
    def consume(self, partition: Partition, offset: int) -> Optional[Message[TPayload]]:
        """
        Consume a message from the provided partition, reading from the given
        offset. If no message exists at the given offset when reading from
        the tail of the partition, this method returns ``None``.

        If the offset is out of range (there are no messages, and we're not
        reading from the tail of the partition where the next message would
        be if it existed), an ``OffsetOutOfRange`` exception will be raised.

        If the topic does not exist, a ``TopicDoesNotExist`` exception will
        be raised. If the topic exists but the partition does not, a
        ``PartitionDoesNotExist`` exception will be raised.
        """
        raise NotImplementedError

    @abstractmethod
    def produce(
        self, partition: Partition, payload: TPayload, timestamp: datetime
    ) -> Message[TPayload]:
        """
        Produce a single message to the provided partition.

        If the topic does not exist, a ``TopicDoesNotExist`` exception will
        be raised. If the topic exists but the partition does not, a
        ``PartitionDoesNotExist`` exception will be raised.
        """
        raise NotImplementedError
