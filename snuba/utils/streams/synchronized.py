from dataclasses import dataclass
from typing import Callable, Mapping, Optional, Sequence

from snuba.utils.streams.consumer import Consumer
from snuba.utils.streams.producer import Producer
from snuba.utils.streams.types import Message, Partition, Topic, TPayload


@dataclass(frozen=True)
class Commit:
    __slots__ = ["group", "partition", "offset"]

    group: str
    partition: Partition
    offset: int


class ConsumerWithCommitLog(Consumer[TPayload]):
    def __init__(
        self,
        consumer: Consumer[TPayload],
        producer: Producer[Commit],
        commit_log_topic: Topic,
    ) -> None:
        self.__consumer = consumer
        self.__producer = producer
        self.__commit_log_topic = commit_log_topic

    def subscribe(
        self,
        topics: Sequence[Topic],
        on_assign: Optional[Callable[[Mapping[Partition, int]], None]] = None,
        on_revoke: Optional[Callable[[Sequence[Partition]], None]] = None,
    ) -> None:
        return self.__consumer.subscribe(topics, on_assign, on_revoke)

    def poll(self, timeout: Optional[float] = None) -> Optional[Message[TPayload]]:
        return self.__consumer.poll(timeout)

    def stage_offsets(self, offsets: Mapping[Partition, int]) -> None:
        return self.__consumer.stage_offsets(offsets)

    def commit_offsets(self) -> Mapping[Partition, int]:
        offsets = super().commit_offsets()

        for partition, offset in offsets.items():
            self.__producer.produce(
                self.__commit_log_topic,
                Commit(self.__consumer.group_id, partition, offset),
            )

        return offsets

    def close(self, timeout: Optional[float] = None) -> None:
        super().close()
        self.__producer.close().result(timeout)
