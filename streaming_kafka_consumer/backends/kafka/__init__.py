from __future__ import annotations

import logging
from concurrent.futures import Future
from datetime import datetime
from enum import Enum
from functools import partial
from pickle import PickleBuffer
from threading import Event
from typing import (
    Any,
    Callable,
    Mapping,
    MutableMapping,
    MutableSequence,
    NamedTuple,
    Optional,
    Sequence,
    Set,
    Tuple,
    Type,
    Union,
)

from confluent_kafka import (
    OFFSET_BEGINNING,
    OFFSET_END,
    OFFSET_INVALID,
    OFFSET_STORED,
    TIMESTAMP_NOT_AVAILABLE,
)
from confluent_kafka import Consumer as ConfluentConsumer
from confluent_kafka import KafkaError
from confluent_kafka import Message as ConfluentMessage
from confluent_kafka import Producer as ConfluentProducer
from confluent_kafka import TopicPartition as ConfluentTopicPartition
from streaming_kafka_consumer.backends.abstract import (
    Consumer,
    ConsumerError,
    EndOfPartition,
    OffsetOutOfRange,
    Producer,
)
from streaming_kafka_consumer.retries import NoRetryPolicy, RetryPolicy
from streaming_kafka_consumer.types import Message, Partition, Topic

from snuba.utils.concurrent import execute

logger = logging.getLogger(__name__)


class TransportError(ConsumerError):
    pass


KafkaConsumerState = Enum(
    "KafkaConsumerState", ["CONSUMING", "ERROR", "CLOSED", "ASSIGNING", "REVOKING"]
)


class InvalidState(RuntimeError):
    def __init__(self, state: KafkaConsumerState):
        self.__state = state


Headers = Sequence[Tuple[str, bytes]]


class KafkaPayload(NamedTuple):
    key: Optional[bytes]
    value: bytes
    headers: Headers

    def __reduce_ex__(self, protocol: int) -> Tuple[Type[KafkaPayload], Any]:
        if protocol >= 5:
            return (
                type(self),
                (self.key, PickleBuffer(self.value), self.headers),
            )
        else:
            return type(self), (self.key, self.value, self.headers)


def as_kafka_configuration_bool(value: Any) -> bool:
    """
    Convert a value to a Python boolean, using the same conversions used by
    the librdkafka configuration parser.
    """
    # https://github.com/edenhill/librdkafka/blob/c8293af/src/rdkafka_conf.c#L1633-L1660
    if isinstance(value, str):
        value = value.lower()
        if value in {"true", "t", "1"}:
            return True
        elif value in {"false", "f", "0"}:
            return False
        else:
            raise ValueError(f"cannot interpret {value!r} as boolean")
    elif isinstance(value, bool):
        return value
    elif isinstance(value, int):
        if value == 1:
            return True
        elif value == 0:
            return False
        else:
            raise ValueError(f"cannot interpret {value!r} as boolean")

    raise TypeError(f"cannot interpret {value!r} as boolean")


class KafkaConsumer(Consumer[KafkaPayload]):
    """
    The behavior of this consumer differs slightly from the Confluent
    consumer during rebalancing operations. Whenever a partition is assigned
    to this consumer, offsets are *always* automatically reset to the
    committed offset for that partition (or if no offsets have been committed
    for that partition, the offset is reset in accordance with the
    ``auto.offset.reset`` configuration value.) This causes partitions that
    are maintained across a rebalance to have the same offset management
    behavior as a partition that is moved from one consumer to another. To
    prevent uncommitted messages from being consumed multiple times,
    ``commit`` should be called in the partition revocation callback.

    The behavior of ``auto.offset.reset`` also differs slightly from the
    Confluent consumer as well: offsets are only reset during initial
    assignment or subsequent rebalancing operations. Any other circumstances
    that would otherwise lead to preemptive offset reset (e.g. the consumer
    tries to read a message that is before the earliest offset, or the
    consumer attempts to read a message that is after the latest offset) will
    cause an exception to be thrown, rather than resetting the offset, as
    this could lead to chunks messages being replayed or skipped, depending
    on the circumstances. This also means that if the committed offset is no
    longer available (such as when reading older messages from the log and
    those messages expire, or reading newer messages from the log and the
    leader crashes and partition ownership fails over to an out-of-date
    replica), the consumer will fail-stop rather than reset to the value of
    ``auto.offset.reset``.
    """

    # Set of logical offsets that do not correspond to actual log positions.
    # These offsets should be considered an implementation detail of the Kafka
    # consumer and not used publically.
    # https://github.com/confluentinc/confluent-kafka-python/blob/443177e1c83d9b66ce30f5eb8775e062453a738b/tests/test_enums.py#L22-L25
    LOGICAL_OFFSETS = frozenset(
        [OFFSET_BEGINNING, OFFSET_END, OFFSET_STORED, OFFSET_INVALID]
    )

    def __init__(
        self,
        configuration: Mapping[str, Any],
        *,
        commit_retry_policy: Optional[RetryPolicy] = None,
    ) -> None:
        if commit_retry_policy is None:
            commit_retry_policy = NoRetryPolicy()

        auto_offset_reset = configuration.get("auto.offset.reset", "largest")
        if auto_offset_reset in {"smallest", "earliest", "beginning"}:
            self.__resolve_partition_starting_offset = (
                self.__resolve_partition_offset_earliest
            )
        elif auto_offset_reset in {"largest", "latest", "end"}:
            self.__resolve_partition_starting_offset = (
                self.__resolve_partition_offset_latest
            )
        elif auto_offset_reset == "error":
            self.__resolve_partition_starting_offset = (
                self.__resolve_partition_offset_error
            )
        else:
            raise ValueError("invalid value for 'auto.offset.reset' configuration")

        if (
            as_kafka_configuration_bool(configuration.get("enable.auto.commit", "true"))
            is not False
        ):
            raise ValueError("invalid value for 'enable.auto.commit' configuration")

        if (
            as_kafka_configuration_bool(
                configuration.get("enable.auto.offset.store", "true")
            )
            is not False
        ):
            raise ValueError(
                "invalid value for 'enable.auto.offset.store' configuration"
            )

        # NOTE: Offsets are explicitly managed as part of the assignment
        # callback, so preemptively resetting offsets is not enabled.
        self.__consumer = ConfluentConsumer(
            {**configuration, "auto.offset.reset": "error"}
        )

        self.__offsets: MutableMapping[Partition, int] = {}
        self.__staged_offsets: MutableMapping[Partition, int] = {}
        self.__paused: Set[Partition] = set()

        self.__commit_retry_policy = commit_retry_policy

        self.__state = KafkaConsumerState.CONSUMING

    def __resolve_partition_offset_earliest(
        self, partition: ConfluentTopicPartition
    ) -> ConfluentTopicPartition:
        low, high = self.__consumer.get_watermark_offsets(partition)
        return ConfluentTopicPartition(partition.topic, partition.partition, low)

    def __resolve_partition_offset_latest(
        self, partition: ConfluentTopicPartition
    ) -> ConfluentTopicPartition:
        low, high = self.__consumer.get_watermark_offsets(partition)
        return ConfluentTopicPartition(partition.topic, partition.partition, high)

    def __resolve_partition_offset_error(
        self, partition: ConfluentTopicPartition
    ) -> ConfluentTopicPartition:
        raise ConsumerError("unable to resolve partition offsets")

    def subscribe(
        self,
        topics: Sequence[Topic],
        on_assign: Optional[Callable[[Mapping[Partition, int]], None]] = None,
        on_revoke: Optional[Callable[[Sequence[Partition]], None]] = None,
    ) -> None:
        """
        Subscribe to topics. This replaces a previous subscription.

        This method does not block. The subscription may not be fulfilled
        immediately: instead, the ``on_assign`` and ``on_revoke`` callbacks
        are called when the subscription state changes with the updated
        assignment for this consumer.

        If provided, the ``on_assign`` callback is called with a mapping of
        partitions to their offsets (at this point, the working offset and the
        committed offset are the same for each partition) on each subscription
        change. Similarly, the ``on_revoke`` callback (if provided) is called
        with a sequence of partitions that are being removed from this
        consumer's assignment. (This callback does not include the offsets,
        as the working offset and committed offset may differ, in some cases
        by substantial margin.)

        Raises an ``InvalidState`` exception if called on a closed consumer.
        """
        if self.__state is not KafkaConsumerState.CONSUMING:
            raise InvalidState(self.__state)

        def assignment_callback(
            consumer: ConfluentConsumer, partitions: Sequence[ConfluentTopicPartition]
        ) -> None:
            self.__state = KafkaConsumerState.ASSIGNING

            try:
                assignment: MutableSequence[ConfluentTopicPartition] = []

                for partition in self.__consumer.committed(partitions):
                    if partition.offset >= 0:
                        assignment.append(partition)
                    elif partition.offset == OFFSET_INVALID:
                        assignment.append(
                            self.__resolve_partition_starting_offset(partition)
                        )
                    else:
                        raise ValueError("received unexpected offset")

                offsets: MutableMapping[Partition, int] = {
                    Partition(Topic(i.topic), i.partition): i.offset for i in assignment
                }
                self.__seek(offsets)

                # Ensure that all partitions are resumed on assignment to avoid
                # carrying over state from a previous assignment.
                self.__consumer.resume(
                    [
                        ConfluentTopicPartition(
                            partition.topic.name, partition.index, offset
                        )
                        for partition, offset in offsets.items()
                    ]
                )

                for partition in offsets:
                    self.__paused.discard(partition)
            except Exception:
                self.__state = KafkaConsumerState.ERROR
                raise

            try:
                if on_assign is not None:
                    on_assign(offsets)
            finally:
                self.__state = KafkaConsumerState.CONSUMING

        def revocation_callback(
            consumer: ConfluentConsumer, partitions: Sequence[ConfluentTopicPartition]
        ) -> None:
            self.__state = KafkaConsumerState.REVOKING

            partitions = [Partition(Topic(i.topic), i.partition) for i in partitions]

            try:
                if on_revoke is not None:
                    on_revoke(partitions)
            finally:
                for partition in partitions:
                    # Staged offsets are deleted during partition revocation to
                    # prevent later committing offsets for partitions that are
                    # no longer owned by this consumer.
                    if partition in self.__staged_offsets:
                        logger.warning(
                            "Dropping staged offset for revoked partition (%r)!",
                            partition,
                        )
                        del self.__staged_offsets[partition]

                    try:
                        self.__offsets.pop(partition)
                    except KeyError:
                        # If there was an error during assignment, this
                        # partition may have never been added to the offsets
                        # mapping.
                        logger.warning(
                            "failed to delete offset for unknown partition: %r",
                            partition,
                        )

                    self.__paused.discard(partition)

                self.__state = KafkaConsumerState.CONSUMING

        self.__consumer.subscribe(
            [topic.name for topic in topics],
            on_assign=assignment_callback,
            on_revoke=revocation_callback,
        )

    def unsubscribe(self) -> None:
        """
        Unsubscribe from topics.

        Raises an ``InvalidState`` exception if called on a closed consumer.
        """
        if self.__state is not KafkaConsumerState.CONSUMING:
            raise InvalidState(self.__state)

        self.__consumer.unsubscribe()

    def poll(self, timeout: Optional[float] = None) -> Optional[Message[KafkaPayload]]:
        """
        Return the next message available to be consumed, if one is
        available. If no message is available, this method will block up to
        the ``timeout`` value before returning ``None``. A timeout of
        ``0.0`` represents "do not block", while a timeout of ``None``
        represents "block until a message is available (or forever)".

        Calling this method may also invoke subscription state change
        callbacks.

        This method may also raise an ``EndOfPartition`` error (a subtype of
        ``ConsumerError``) when the consumer has reached the end of a
        partition that it is subscribed to and no additional messages are
        available. The ``partition`` attribute of the raised exception
        specifies the end which partition has been reached. (Since this
        consumer is multiplexing a set of partitions, this exception does not
        mean that *all* of the partitions that the consumer is subscribed to
        do not have any messages, just that it has reached the end of one of
        them. This also does not mean that additional messages won't be
        available in future poll calls.) Not every backend implementation
        supports this feature or is configured to raise in this scenario.

        Raises an ``InvalidState`` exception if called on a closed consumer.

        Raises a ``TransportError`` for various other consumption-related
        errors.
        """
        if self.__state is not KafkaConsumerState.CONSUMING:
            raise InvalidState(self.__state)

        message: Optional[ConfluentMessage] = self.__consumer.poll(
            *[timeout] if timeout is not None else []
        )
        if message is None:
            return None

        error: Optional[KafkaError] = message.error()
        if error is not None:
            code = error.code()
            if code == KafkaError._PARTITION_EOF:
                raise EndOfPartition(
                    Partition(Topic(message.topic()), message.partition()),
                    message.offset(),
                )
            elif code == KafkaError._TRANSPORT:
                raise TransportError(str(error))
            elif code == KafkaError.OFFSET_OUT_OF_RANGE:
                raise OffsetOutOfRange(str(error))
            else:
                raise ConsumerError(str(error))

        headers: Optional[Headers] = message.headers()
        result = Message(
            Partition(Topic(message.topic()), message.partition()),
            message.offset(),
            KafkaPayload(
                message.key(), message.value(), headers if headers is not None else [],
            ),
            datetime.utcfromtimestamp(message.timestamp()[1] / 1000.0),
        )

        self.__offsets[result.partition] = result.next_offset

        return result

    def tell(self) -> Mapping[Partition, int]:
        """
        Return the read offsets for all assigned partitions.

        Raises an ``InvalidState`` if called on a closed consumer.
        """
        if self.__state in {KafkaConsumerState.CLOSED, KafkaConsumerState.ERROR}:
            raise InvalidState(self.__state)

        return self.__offsets

    def __validate_offsets(self, offsets: Mapping[Partition, int]) -> None:
        invalid_offsets: Mapping[Partition, int] = {
            partition: offset for partition, offset in offsets.items() if offset < 0
        }

        if invalid_offsets:
            raise ConsumerError(f"invalid offsets: {invalid_offsets!r}")

    def __seek(self, offsets: Mapping[Partition, int]) -> None:
        self.__validate_offsets(offsets)

        if self.__state is KafkaConsumerState.ASSIGNING:
            # Calling ``seek`` on the Confluent consumer from an assignment
            # callback will throw an "Erroneous state" error. Instead,
            # partition offsets have to be initialized by calling ``assign``.
            self.__consumer.assign(
                [
                    ConfluentTopicPartition(
                        partition.topic.name, partition.index, offset
                    )
                    for partition, offset in offsets.items()
                ]
            )
        else:
            for partition, offset in offsets.items():
                self.__consumer.seek(
                    ConfluentTopicPartition(
                        partition.topic.name, partition.index, offset
                    )
                )

        self.__offsets.update(offsets)

    def seek(self, offsets: Mapping[Partition, int]) -> None:
        """
        Change the read offsets for the provided partitions.

        Raises an ``InvalidState`` if called on a closed consumer.
        """
        if self.__state in {KafkaConsumerState.CLOSED, KafkaConsumerState.ERROR}:
            raise InvalidState(self.__state)

        if offsets.keys() - self.__offsets.keys():
            raise ConsumerError("cannot seek on unassigned partitions")

        self.__seek(offsets)

    def pause(self, partitions: Sequence[Partition]) -> None:
        """
        Pause the consumption of messages for the provided partitions.

        Raises an ``InvalidState`` if called on a closed consumer.
        """
        if self.__state in {KafkaConsumerState.CLOSED, KafkaConsumerState.ERROR}:
            raise InvalidState(self.__state)

        if set(partitions) - self.__offsets.keys():
            raise ConsumerError("cannot pause unassigned partitions")

        self.__consumer.pause(
            [
                ConfluentTopicPartition(partition.topic.name, partition.index)
                for partition in partitions
            ]
        )

        self.__paused.update(partitions)

        # XXX: Seeking to a specific partition offset and immediately pausing
        # that partition causes the seek to be ignored for some reason.
        self.seek(
            {
                partition: offset
                for partition, offset in self.__offsets.items()
                if partition in partitions
            }
        )

    def resume(self, partitions: Sequence[Partition]) -> None:
        """
        Resume the consumption of messages for the provided partitions.

        Raises an ``InvalidState`` if called on a closed consumer.
        """
        if self.__state in {KafkaConsumerState.CLOSED, KafkaConsumerState.ERROR}:
            raise InvalidState(self.__state)

        if set(partitions) - self.__offsets.keys():
            raise ConsumerError("cannot resume unassigned partitions")

        self.__consumer.resume(
            [
                ConfluentTopicPartition(partition.topic.name, partition.index)
                for partition in partitions
            ]
        )

        for partition in partitions:
            self.__paused.discard(partition)

    def paused(self) -> Sequence[Partition]:
        if self.__state in {KafkaConsumerState.CLOSED, KafkaConsumerState.ERROR}:
            raise InvalidState(self.__state)

        return [*self.__paused]

    def stage_offsets(self, offsets: Mapping[Partition, int]) -> None:
        if self.__state in {KafkaConsumerState.CLOSED, KafkaConsumerState.ERROR}:
            raise InvalidState(self.__state)

        if offsets.keys() - self.__offsets.keys():
            raise ConsumerError("cannot stage offsets for unassigned partitions")

        self.__validate_offsets(offsets)

        # TODO: Maybe log a warning if these offsets exceed the current
        # offsets, since that's probably a side effect of an incorrect usage
        # pattern?
        self.__staged_offsets.update(offsets)

    def __commit(self) -> Mapping[Partition, int]:
        if self.__state in {KafkaConsumerState.CLOSED, KafkaConsumerState.ERROR}:
            raise InvalidState(self.__state)

        result: Optional[Sequence[ConfluentTopicPartition]]

        if self.__staged_offsets:
            result = self.__consumer.commit(
                offsets=[
                    ConfluentTopicPartition(
                        partition.topic.name, partition.index, offset
                    )
                    for partition, offset in self.__staged_offsets.items()
                ],
                asynchronous=False,
            )
        else:
            result = []

        assert result is not None  # synchronous commit should return result immediately

        self.__staged_offsets.clear()

        offsets: MutableMapping[Partition, int] = {}

        for value in result:
            # The Confluent Kafka Consumer will include logical offsets in the
            # sequence of ``Partition`` objects returned by ``commit``. These
            # are an implementation detail of the Kafka Consumer, so we don't
            # expose them here.
            # NOTE: These should no longer be seen now that we are forcing
            # offsets to be set as part of the assignment callback.
            if value.offset in self.LOGICAL_OFFSETS:
                continue

            assert value.offset >= 0, "expected non-negative offset"
            offsets[Partition(Topic(value.topic), value.partition)] = value.offset

        return offsets

    def commit_offsets(self) -> Mapping[Partition, int]:
        """
        Commit staged offsets for all partitions that this consumer is
        assigned to. The return value of this method is a mapping of
        partitions with their committed offsets as values.

        Raises an ``InvalidState`` if called on a closed consumer.
        """
        return self.__commit_retry_policy.call(self.__commit)

    def close(self, timeout: Optional[float] = None) -> None:
        """
        Close the consumer. This stops consuming messages, *may* commit
        staged offsets (depending on the configuration), and ends its
        subscription.

        Raises a ``InvalidState`` if the consumer is unable to be closed
        before the timeout is reached.
        """
        try:
            self.__consumer.close()
        except RuntimeError:
            pass

        self.__state = KafkaConsumerState.CLOSED

    @property
    def closed(self) -> bool:
        return self.__state is KafkaConsumerState.CLOSED


class KafkaProducer(Producer[KafkaPayload]):
    def __init__(self, configuration: Mapping[str, Any]) -> None:
        self.__configuration = configuration

        self.__producer = ConfluentProducer(configuration)
        self.__shutdown_requested = Event()

        # The worker must execute in a separate thread to ensure that callbacks
        # are fired -- otherwise trying to produce "synchronously" via
        # ``produce(...).result()`` could result in a deadlock.
        self.__result = execute(self.__worker)

    def __worker(self) -> None:
        """
        Continuously polls the producer to ensure that delivery callbacks are
        triggered (which correspondingly set the result values on the
        ``Future`` instances returned by ``produce``.) This function exits
        after a shutdown request has been issued (via ``close``) and all
        in-flight messages have been delivered.
        """
        while not self.__shutdown_requested.is_set():
            self.__producer.poll(0.1)
        self.__producer.flush()

    def __delivery_callback(
        self,
        future: Future[Message[KafkaPayload]],
        payload: KafkaPayload,
        error: KafkaError,
        message: ConfluentMessage,
    ) -> None:
        if error is not None:
            future.set_exception(TransportError(error))
        else:
            try:
                timestamp_type, timestamp_value = message.timestamp()
                if timestamp_type is TIMESTAMP_NOT_AVAILABLE:
                    raise ValueError("timestamp not available")

                future.set_result(
                    Message(
                        Partition(Topic(message.topic()), message.partition()),
                        message.offset(),
                        payload,
                        datetime.utcfromtimestamp(timestamp_value / 1000.0),
                    )
                )
            except Exception as error:
                future.set_exception(error)

    def produce(
        self, destination: Union[Topic, Partition], payload: KafkaPayload,
    ) -> Future[Message[KafkaPayload]]:
        if self.__shutdown_requested.is_set():
            raise RuntimeError("producer has been closed")

        if isinstance(destination, Topic):
            produce = partial(self.__producer.produce, topic=destination.name)
        elif isinstance(destination, Partition):
            produce = partial(
                self.__producer.produce,
                topic=destination.topic.name,
                partition=destination.index,
            )
        else:
            raise TypeError("invalid destination type")

        future: Future[Message[KafkaPayload]] = Future()
        future.set_running_or_notify_cancel()
        produce(
            value=payload.value,
            key=payload.key,
            headers=payload.headers,
            on_delivery=partial(self.__delivery_callback, future, payload),
        )
        return future

    def close(self) -> Future[None]:
        self.__shutdown_requested.set()
        return self.__result
