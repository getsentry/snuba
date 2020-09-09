from __future__ import annotations

from collections import defaultdict, deque
from concurrent.futures import Future
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
    Union,
)

from snuba.utils.streams.backends.abstract import (
    Consumer,
    ConsumerError,
    EndOfPartition,
    Producer,
)
from snuba.utils.streams.backends.local.storages.abstract import MessageStorage
from snuba.utils.streams.types import Message, Partition, Topic, TPayload


class LocalBroker(Generic[TPayload]):
    def __init__(self, message_storage: MessageStorage[TPayload]) -> None:
        self.__message_storage = message_storage

        self.__offsets: MutableMapping[
            str, MutableMapping[Partition, int]
        ] = defaultdict(dict)

        # The active subscriptions are stored by consumer group as a mapping
        # between the consumer and it's subscribed topics.
        self.__subscriptions: MutableMapping[
            str, MutableMapping[LocalConsumer[TPayload], Sequence[Topic]]
        ] = defaultdict(dict)

        self.__lock = Lock()

    def get_consumer(
        self, group: str, enable_end_of_partition: bool = False
    ) -> Consumer[TPayload]:
        return LocalConsumer(
            self, group, enable_end_of_partition=enable_end_of_partition
        )

    def get_producer(self) -> Producer[TPayload]:
        return LocalProducer(self)

    def create_topic(self, topic: Topic, partitions: int) -> None:
        with self.__lock:
            self.__message_storage.create_topic(topic, partitions)

    def produce(self, partition: Partition, payload: TPayload) -> Message[TPayload]:
        with self.__lock:
            return self.__message_storage.produce(partition, payload)

    def subscribe(
        self, consumer: LocalConsumer[TPayload], topics: Sequence[Topic]
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

            for topic in set(topics):
                try:
                    partition_count = self.__message_storage.get_partition_count(topic)
                except KeyError:
                    continue

                for index in range(partition_count):
                    partition = Partition(topic, index)
                    # TODO: Handle offset reset more realistically.
                    assignment[partition] = self.__offsets[consumer.group].get(
                        partition, 0
                    )

        return assignment

    def unsubscribe(self, consumer: LocalConsumer[TPayload]) -> Sequence[Partition]:
        with self.__lock:
            partitions: MutableSequence[Partition] = []
            for topic in self.__subscriptions[consumer.group].pop(consumer):
                partitions.extend(
                    Partition(topic, i)
                    for i in range(self.__message_storage.get_partition_count(topic))
                )
            return partitions

    def consume(self, partition: Partition, offset: int) -> Optional[Message[TPayload]]:
        with self.__lock:
            return self.__message_storage.consume(partition, offset)

    def commit(
        self, consumer: LocalConsumer[TPayload], offsets: Mapping[Partition, int]
    ) -> None:
        with self.__lock:
            # TODO: This could possibly use more validation?
            self.__offsets[consumer.group].update(offsets)


class Subscription(NamedTuple):
    topics: Sequence[Topic]
    assignment_callback: Optional[Callable[[Mapping[Partition, int]], None]]
    revocation_callback: Optional[Callable[[Sequence[Partition]], None]]


class LocalConsumer(Consumer[TPayload]):
    def __init__(
        self,
        broker: LocalBroker[TPayload],
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
                    self.__offsets[partition] = message.next_offset
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


class LocalProducer(Producer[TPayload]):
    def __init__(self, broker: LocalBroker[TPayload]) -> None:
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
