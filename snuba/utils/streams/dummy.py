from __future__ import annotations

from collections import defaultdict
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
    Set,
    Union,
)

from snuba.utils.streams.consumer import Consumer, ConsumerError, EndOfPartition
from snuba.utils.streams.producer import Producer
from snuba.utils.streams.types import Message, Partition, Topic, TPayload

epoch = datetime(2019, 12, 19)


class DummyBroker(Generic[TPayload]):
    def __init__(
        self, topics: MutableMapping[Topic, Sequence[MutableSequence[TPayload]]]
    ) -> None:
        self.topics = topics
        self.offsets: MutableMapping[str, MutableMapping[Partition, int]] = defaultdict(
            dict
        )
        # The active subscriptions are stored by consumer group as a mapping
        # between the consumer and it's subscribed topics.
        self.subscriptions: MutableMapping[
            str, MutableMapping[DummyConsumer[TPayload], Sequence[Topic]]
        ] = defaultdict(dict)

    def subscribe(
        self, consumer: DummyConsumer[TPayload], topics: Sequence[Topic]
    ) -> Sequence[Partition]:
        if self.subscriptions[consumer.group]:
            # XXX: Consumer group balancing is not currently implemented.
            if consumer not in self.subscriptions[consumer.group]:
                raise NotImplementedError

            # XXX: Updating an existing subscription is currently not implemented.
            if self.subscriptions[consumer.group][consumer] != topics:
                raise NotImplementedError

        self.subscriptions[consumer.group][consumer] = topics

        assignment: MutableSequence[Partition] = []

        for topic in self.topics.keys() & set(topics):
            assignment.extend(
                [Partition(topic, index) for index in range(len(self.topics[topic]))]
            )

        return assignment

    def unsubscribe(self, consumer: DummyConsumer[TPayload]) -> None:
        del self.subscriptions[consumer.group][consumer]


class DummyConsumer(Consumer[TPayload]):
    def __init__(
        self,
        broker: DummyBroker[TPayload],
        group: str,
        enable_end_of_partition: bool = False,
    ) -> None:
        self.__broker = broker
        self.__group = group

        self.__subscription: Sequence[Topic] = []
        self.__assignment: Optional[Sequence[Partition]] = None

        self.__offsets: MutableMapping[Partition, int] = {}
        self.__staged_offsets: MutableMapping[Partition, int] = {}

        self.__paused: Set[Partition] = set()

        # The offset that a the last ``EndOfPartition`` exception that was
        # raised at. To maintain consistency with the Confluent consumer, this
        # is only sent once per (partition, offset) pair.
        self.__enable_end_of_partition = enable_end_of_partition
        self.__last_eof_at: MutableMapping[Partition, int] = {}

        self.commit_offsets_calls = 0
        self.close_calls = 0

        self.__closed = False

    @property
    def group(self) -> str:
        return self.__group

    def subscribe(
        self,
        topics: Sequence[Topic],
        on_assign: Optional[Callable[[Mapping[Partition, int]], None]] = None,
        on_revoke: Optional[Callable[[Sequence[Partition]], None]] = None,
    ) -> None:
        if self.__closed:
            raise RuntimeError("consumer is closed")

        # TODO: Handle offset reset more realistically.
        self.__offsets = {
            partition: self.__broker.offsets[self.__group].get(partition, 0)
            for partition in self.__broker.subscribe(self, topics)
        }

        self.__staged_offsets.clear()
        self.__last_eof_at.clear()

        if on_assign is not None:
            on_assign(self.__offsets)

    def unsubscribe(self) -> None:
        if self.__closed:
            raise RuntimeError("consumer is closed")

        self.__broker.unsubscribe(self)

        self.__offsets = {}
        self.__staged_offsets.clear()
        self.__last_eof_at.clear()

    def poll(self, timeout: Optional[float] = None) -> Optional[Message[TPayload]]:
        if self.__closed:
            raise RuntimeError("consumer is closed")

        # TODO: Throw ``EndOfPartition`` errors.
        for partition, offset in sorted(self.__offsets.items()):
            if partition in self.__paused:
                continue  # skip paused partitions

            messages = self.__broker.topics[partition.topic][partition.index]
            try:
                payload = messages[offset]
            except IndexError:
                if offset == len(messages):
                    if (
                        self.__enable_end_of_partition
                        and offset > self.__last_eof_at.get(partition, 0)
                    ):
                        self.__last_eof_at[partition] = offset
                        raise EndOfPartition(partition, offset)
                else:
                    raise ConsumerError("invalid offset")
            else:
                message = Message(partition, offset, payload, epoch)
                self.__offsets[partition] = message.get_next_offset()
                return message

        return None

    def pause(self, partitions: Sequence[Partition]) -> None:
        if self.__closed:
            raise RuntimeError("consumer is closed")

        if set(partitions) - self.__offsets.keys():
            raise ConsumerError("cannot pause unassigned partitions")

        self.__paused.update(partitions)

    def resume(self, partitions: Sequence[Partition]) -> None:
        if self.__closed:
            raise RuntimeError("consumer is closed")

        if set(partitions) - self.__offsets.keys():
            raise ConsumerError("cannot resume unassigned partitions")

        for partition in partitions:
            self.__paused.discard(partition)

    def paused(self) -> Sequence[Partition]:
        if self.__closed:
            raise RuntimeError("consumer is closed")

        return [*self.__paused]

    def tell(self) -> Mapping[Partition, int]:
        if self.__closed:
            raise RuntimeError("consumer is closed")

        return self.__offsets

    def __validate_offsets(self, offsets: Mapping[Partition, int]) -> None:
        invalid_offsets: Mapping[Partition, int] = {
            partition: offset for partition, offset in offsets.items() if offset < 0
        }

        if invalid_offsets:
            raise ConsumerError(f"invalid offsets: {invalid_offsets!r}")

    def seek(self, offsets: Mapping[Partition, int]) -> None:
        if self.__closed:
            raise RuntimeError("consumer is closed")

        if offsets.keys() - self.__offsets.keys():
            raise ConsumerError("cannot seek on unassigned partitions")

        self.__validate_offsets(offsets)

        self.__offsets.update(offsets)

    def stage_offsets(self, offsets: Mapping[Partition, int]) -> None:
        if self.__closed:
            raise RuntimeError("consumer is closed")

        if offsets.keys() - self.__offsets.keys():
            raise ConsumerError("cannot stage offsets for unassigned partitions")

        self.__validate_offsets(offsets)

        self.__staged_offsets.update(offsets)

    def commit_offsets(self) -> Mapping[Partition, int]:
        if self.__closed:
            raise RuntimeError("consumer is closed")

        offsets = {**self.__staged_offsets}
        self.__broker.offsets[self.__group].update(offsets)
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
    ) -> Future[Message[TPayload]]:
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

        future: Future[Message[TPayload]] = Future()
        future.set_running_or_notify_cancel()
        future.set_result(Message(partition, offset, payload, epoch))
        return future

    def close(self) -> Future[None]:
        self.__closed = True

        future: Future[None] = Future()
        future.set_running_or_notify_cancel()
        future.set_result(None)
        return future
