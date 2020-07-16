from __future__ import annotations

from collections import defaultdict, deque
from concurrent.futures import Future
from datetime import datetime
from functools import partial
from threading import Lock, RLock
from typing import (
    Callable,
    Deque,
    Generic,
    Mapping,
    MutableMapping,
    MutableSequence,
    NamedTuple,
    Optional,
    Sequence,
    Set,
    Tuple,
    Union,
)

from snuba.utils.clock import Clock, TestingClock
from snuba.utils.streams.consumer import Consumer, ConsumerError, EndOfPartition
from snuba.utils.streams.producer import Producer
from snuba.utils.streams.types import Message, Partition, Topic, TPayload

epoch = datetime(2019, 12, 19)


class DummyBroker(Generic[TPayload]):
    def __init__(self, clock: Clock = TestingClock(epoch.timestamp())) -> None:
        self.__clock = clock
        self.__topics: MutableMapping[
            Topic, Sequence[MutableSequence[Tuple[TPayload, datetime]]]
        ] = {}

        self.__offsets: MutableMapping[
            str, MutableMapping[Partition, int]
        ] = defaultdict(dict)

        # The active subscriptions are stored by consumer group as a mapping
        # between the consumer and it's subscribed topics.
        self.__subscriptions: MutableMapping[
            str, MutableMapping[DummyConsumer[TPayload], Sequence[Topic]]
        ] = defaultdict(dict)

        self.__lock = Lock()

    def create_topic(self, topic: Topic, partitions: int) -> None:
        with self.__lock:
            if topic in self.__topics:
                raise ValueError("topic already exists")

            self.__topics[topic] = [[] for i in range(partitions)]

    def produce(self, partition: Partition, payload: TPayload) -> Message[TPayload]:
        with self.__lock:
            messages = self.__topics[partition.topic][partition.index]
            offset = len(messages)
            timestamp = datetime.fromtimestamp(self.__clock.time())
            messages.append((payload, timestamp))

        return Message(partition, offset, payload, timestamp)

    def subscribe(
        self, consumer: DummyConsumer[TPayload], topics: Sequence[Topic]
    ) -> Mapping[Partition, int]:
        with self.__lock:
            if self.__subscriptions[consumer.group]:
                # XXX: Consumer group balancing is not currently implemented.
                if consumer not in self.__subscriptions[consumer.group]:
                    raise NotImplementedError

                # XXX: Updating an existing subscription is currently not implemented.
                if self.__subscriptions[consumer.group][consumer] != topics:
                    raise NotImplementedError

            self.__subscriptions[consumer.group][consumer] = topics

            assignment: MutableMapping[Partition, int] = {}

            for topic in self.__topics.keys() & set(topics):
                for index in range(len(self.__topics[topic])):
                    partition = Partition(topic, index)
                    # TODO: Handle offset reset more realistically.
                    assignment[partition] = self.__offsets[consumer.group].get(
                        partition, 0
                    )

        return assignment

    def unsubscribe(self, consumer: DummyConsumer[TPayload]) -> Sequence[Partition]:
        with self.__lock:
            partitions: MutableSequence[Partition] = []
            for topic in self.__subscriptions[consumer.group].pop(consumer):
                partitions.extend(
                    Partition(topic, i) for i in range(len(self.__topics[topic]))
                )
            return partitions

    def consume(self, partition: Partition, offset: int) -> Optional[Message[TPayload]]:
        with self.__lock:
            messages = self.__topics[partition.topic][partition.index]

            try:
                payload, timestamp = messages[offset]
            except IndexError:
                if offset == len(messages):
                    return None
                else:
                    raise Exception("invalid offset")

        return Message(partition, offset, payload, timestamp)

    def commit(
        self, consumer: DummyConsumer[TPayload], offsets: Mapping[Partition, int]
    ) -> None:
        with self.__lock:
            # TODO: This could possibly use more validation?
            self.__offsets[consumer.group].update(offsets)


class Subscription(NamedTuple):
    topics: Sequence[Topic]
    assignment_callback: Optional[Callable[[Mapping[Partition, int]], None]]
    revocation_callback: Optional[Callable[[Sequence[Partition]], None]]


class DummyConsumer(Consumer[TPayload]):
    def __init__(
        self,
        broker: DummyBroker[TPayload],
        group: str,
        enable_end_of_partition: bool = False,
    ) -> None:
        self.__broker = broker
        self.__group = group

        self.__subscription: Optional[Subscription] = None
        self.__pending_callbacks: Deque[Callable[[], None]] = deque()

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

        # The lock used must be reentrant to avoid deadlocking when calling
        # methods from assignment callbacks.
        self.__lock = RLock()
        self.__closed = False

    @property
    def group(self) -> str:
        return self.__group

    def __assign(
        self, subscription: Subscription, offsets: Mapping[Partition, int]
    ) -> None:
        self.__offsets = {**offsets}
        if subscription.assignment_callback is not None:
            subscription.assignment_callback(offsets)

    def __revoke(
        self, subscription: Subscription, partitions: Sequence[Partition]
    ) -> None:
        if subscription.revocation_callback is not None:
            subscription.revocation_callback(partitions)
        self.__offsets = {}

    def subscribe(
        self,
        topics: Sequence[Topic],
        on_assign: Optional[Callable[[Mapping[Partition, int]], None]] = None,
        on_revoke: Optional[Callable[[Sequence[Partition]], None]] = None,
    ) -> None:
        with self.__lock:
            if self.__closed:
                raise RuntimeError("consumer is closed")

            self.__subscription = Subscription(topics, on_assign, on_revoke)
            self.__pending_callbacks.append(
                partial(
                    self.__assign,
                    self.__subscription,
                    self.__broker.subscribe(self, topics),
                )
            )

            self.__staged_offsets.clear()
            self.__last_eof_at.clear()

    def unsubscribe(self) -> None:
        with self.__lock:
            if self.__closed:
                raise RuntimeError("consumer is closed")

            self.__pending_callbacks.append(
                partial(
                    self.__revoke, self.__subscription, self.__broker.unsubscribe(self),
                )
            )
            self.__subscription = None

            self.__staged_offsets.clear()
            self.__last_eof_at.clear()

    def poll(self, timeout: Optional[float] = None) -> Optional[Message[TPayload]]:
        with self.__lock:
            if self.__closed:
                raise RuntimeError("consumer is closed")

            while self.__pending_callbacks:
                callback = self.__pending_callbacks.popleft()
                callback()

            for partition, offset in sorted(self.__offsets.items()):
                if partition in self.__paused:
                    continue  # skip paused partitions

                try:
                    message = self.__broker.consume(partition, offset)
                except Exception as e:
                    raise ConsumerError("error consuming mesage") from e

                if message is None:
                    if self.__enable_end_of_partition and (
                        partition not in self.__last_eof_at
                        or offset > self.__last_eof_at[partition]
                    ):
                        self.__last_eof_at[partition] = offset
                        raise EndOfPartition(partition, offset)
                else:
                    self.__offsets[partition] = message.get_next_offset()
                    return message

            return None

    def pause(self, partitions: Sequence[Partition]) -> None:
        with self.__lock:
            if self.__closed:
                raise RuntimeError("consumer is closed")

            if set(partitions) - self.__offsets.keys():
                raise ConsumerError("cannot pause unassigned partitions")

            self.__paused.update(partitions)

    def resume(self, partitions: Sequence[Partition]) -> None:
        with self.__lock:
            if self.__closed:
                raise RuntimeError("consumer is closed")

            if set(partitions) - self.__offsets.keys():
                raise ConsumerError("cannot resume unassigned partitions")

            for partition in partitions:
                self.__paused.discard(partition)

    def paused(self) -> Sequence[Partition]:
        with self.__lock:
            if self.__closed:
                raise RuntimeError("consumer is closed")

            return [*self.__paused]

    def tell(self) -> Mapping[Partition, int]:
        with self.__lock:
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
        with self.__lock:
            if self.__closed:
                raise RuntimeError("consumer is closed")

            if offsets.keys() - self.__offsets.keys():
                raise ConsumerError("cannot seek on unassigned partitions")

            self.__validate_offsets(offsets)

            self.__offsets.update(offsets)

    def stage_offsets(self, offsets: Mapping[Partition, int]) -> None:
        with self.__lock:
            if self.__closed:
                raise RuntimeError("consumer is closed")

            if offsets.keys() - self.__offsets.keys():
                raise ConsumerError("cannot stage offsets for unassigned partitions")

            self.__validate_offsets(offsets)

            self.__staged_offsets.update(offsets)

    def commit_offsets(self) -> Mapping[Partition, int]:
        with self.__lock:
            if self.__closed:
                raise RuntimeError("consumer is closed")

            offsets = {**self.__staged_offsets}
            self.__broker.commit(self, offsets)
            self.__staged_offsets.clear()

            self.commit_offsets_calls += 1
            return offsets

    def close(self, timeout: Optional[float] = None) -> None:
        with self.__lock:
            if self.__subscription is not None:
                self.__revoke(self.__subscription, self.__broker.unsubscribe(self))
                self.__subscription = None

            self.__closed = True
            self.close_calls += 1

    @property
    def closed(self) -> bool:
        return self.__closed


class DummyProducer(Producer[TPayload]):
    def __init__(self, broker: DummyBroker[TPayload]) -> None:
        self.__broker = broker

        self.__lock = Lock()
        self.__closed = False

    def produce(
        self, destination: Union[Topic, Partition], payload: TPayload
    ) -> Future[Message[TPayload]]:
        with self.__lock:
            assert not self.__closed

            partition: Partition
            if isinstance(destination, Topic):
                partition = Partition(destination, 0)  # TODO: Randomize?
            elif isinstance(destination, Partition):
                partition = destination
            else:
                raise TypeError("invalid destination type")

            future: Future[Message[TPayload]] = Future()
            future.set_running_or_notify_cancel()
            try:
                message = self.__broker.produce(partition, payload)
                future.set_result(message)
            except Exception as e:
                future.set_exception(e)
            return future

    def close(self) -> Future[None]:
        with self.__lock:
            self.__closed = True

        future: Future[None] = Future()
        future.set_running_or_notify_cancel()
        future.set_result(None)
        return future
