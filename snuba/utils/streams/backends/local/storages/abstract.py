from abc import ABC, abstractmethod
from datetime import datetime
from typing import Generic, Optional

from snuba.utils.streams.types import Message, Partition, Topic, TPayload


class MessageStorage(ABC, Generic[TPayload]):
    @abstractmethod
    def create_topic(self, topic: Topic, partitions: int) -> None:
        raise NotImplementedError

    @abstractmethod
    def get_partition_count(self, topic: Topic) -> int:
        raise NotImplementedError

    @abstractmethod
    def consume(self, partition: Partition, offset: int) -> Optional[Message[TPayload]]:
        raise NotImplementedError

    @abstractmethod
    def produce(
        self, partition: Partition, payload: TPayload, timestamp: datetime
    ) -> Message[TPayload]:
        raise NotImplementedError
