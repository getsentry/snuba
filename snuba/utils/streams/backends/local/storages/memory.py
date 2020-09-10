from datetime import datetime
from typing import MutableMapping, MutableSequence, Optional, Sequence, Tuple

from snuba.utils.streams.backends.local.storages.abstract import MessageStorage
from snuba.utils.streams.types import Message, Partition, Topic, TPayload


class MemoryMessageStorage(MessageStorage[TPayload]):
    def __init__(self) -> None:
        self.__topics: MutableMapping[
            Topic, Sequence[MutableSequence[Tuple[TPayload, datetime]]]
        ] = {}

    def create_topic(self, topic: Topic, partitions: int) -> None:
        if topic in self.__topics:
            raise ValueError("topic already exists")

        self.__topics[topic] = [[] for i in range(partitions)]

    def get_partition_count(self, topic: Topic) -> int:
        return len(self.__topics[topic])

    def consume(self, partition: Partition, offset: int) -> Optional[Message[TPayload]]:
        messages = self.__topics[partition.topic][partition.index]

        try:
            payload, timestamp = messages[offset]
        except IndexError:
            if offset == len(messages):
                return None
            else:
                raise Exception("invalid offset")

        return Message(partition, offset, payload, timestamp)

    def produce(
        self, partition: Partition, payload: TPayload, timestamp: datetime
    ) -> Message[TPayload]:
        messages = self.__topics[partition.topic][partition.index]
        offset = len(messages)
        messages.append((payload, timestamp))
        return Message(partition, offset, payload, timestamp)
