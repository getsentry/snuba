from __future__ import annotations

from concurrent.futures import Future
from datetime import datetime
from typing import (
    Callable,
    Generic,
    Mapping,
    MutableMapping,
    MutableSequence,
    Optional,
    Sequence,
    Union,
)

from snuba.utils.streams.consumer import Consumer, ConsumerError
from snuba.utils.streams.producer import MessageDetails, Producer
from snuba.utils.streams.types import Message, Partition, Topic, TPayload

epoch = datetime(2019, 12, 19)


class DummyBroker(Generic[TPayload]):
    def __init__(
        self, topics: Mapping[Topic, Sequence[MutableSequence[TPayload]]]
    ) -> None:
        self.topics = topics


class DummyConsumer(Consumer[TPayload]):
    def __init__(self, broker: DummyBroker[TPayload]) -> None:
        self.__broker = broker

        self.__subscription: Sequence[Topic] = []
        self.__assignment: Optional[Sequence[Partition]] = None

        self.__offsets: MutableMapping[Partition, int] = {}
        self.__staged_offsets: MutableMapping[Partition, int] = {}
        self.__committed_offsets: MutableMapping[Partition, int] = {}

        self.commit_offsets_calls = 0
        self.close_calls = 0

        self.__closed = False

    def subscribe(
        self,
        topics: Sequence[Topic],
        on_assign: Optional[Callable[[Mapping[Partition, int]], None]] = None,
        on_revoke: Optional[Callable[[Sequence[Partition]], None]] = None,
    ) -> None:
        assert not self.__closed

        self.__subscription = topics

        assignment: MutableSequence[Partition] = []
        for topic, partitions in self.__broker.topics.items():
            if topic not in topics:
                continue

            assignment.extend([Partition(topic, i) for i in range(len(partitions))])

        if self.__assignment is not None and on_revoke is not None:
            on_revoke(self.__assignment)

        self.__assignment = assignment

        # TODO: Handle offset reset more realistically.
        self.__offsets = {
            partition: self.__committed_offsets.get(partition, 0)
            for partition in assignment
        }

        self.__staged_offsets.clear()

        if on_assign is not None:
            on_assign(self.__offsets)

    def poll(self, timeout: Optional[float] = None) -> Optional[Message[TPayload]]:
        assert not self.__closed

        # TODO: Throw ``EndOfPartition`` errors.
        for partition, offset in sorted(self.__offsets.items()):
            messages = self.__broker.topics[partition.topic][partition.index]
            try:
                payload = messages[offset]
            except IndexError:
                if not offset == len(messages):
                    raise ConsumerError("invalid offset")
            else:
                message = Message(partition, offset, payload, epoch)
                self.__offsets[partition] = message.get_next_offset()
                return message

        return None

    def tell(self) -> Mapping[Partition, int]:
        return self.__offsets

    def seek(self, offsets: Mapping[Partition, int]) -> None:
        if offsets.keys() - self.__offsets.keys():
            raise ConsumerError("cannot seek on unassigned partitions")

        self.__offsets.update(offsets)

    def stage_offsets(self, offsets: Mapping[Partition, int]) -> None:
        assert not self.__closed

        self.__staged_offsets.update(offsets)

    def commit_offsets(self) -> Mapping[Partition, int]:
        assert not self.__closed

        offsets = {**self.__staged_offsets}
        self.__committed_offsets.update(offsets)
        self.__staged_offsets.clear()

        self.commit_offsets_calls += 1
        return offsets

    def close(self, timeout: Optional[float] = None) -> None:
        self.__closed = True
        self.close_calls += 1


class DummyProducer(Producer[TPayload]):
    def __init__(self, broker: DummyBroker[TPayload]) -> None:
        self.__broker = broker

        self.__closed = False

    def produce(
        self, destination: Union[Topic, Partition], payload: TPayload
    ) -> Future[MessageDetails]:
        assert not self.__closed

        partition: Partition
        if isinstance(destination, Topic):
            partition = Partition(destination, 0)  # TODO: Randomize?
        elif isinstance(destination, Partition):
            partition = destination
        else:
            raise TypeError("invalid destination type")

        messages = self.__broker.topics[partition.topic][partition.index]
        offset = len(messages)
        messages.append(payload)

        future: Future[MessageDetails] = Future()
        future.set_running_or_notify_cancel()
        future.set_result(MessageDetails(partition, offset))
        return future

    def close(self) -> Future[None]:
        self.__closed = True

        future: Future[None] = Future()
        future.set_running_or_notify_cancel()
        future.set_result(None)
        return future
