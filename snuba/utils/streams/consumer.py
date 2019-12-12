from __future__ import annotations

import logging
from dataclasses import dataclass
from enum import Enum
from typing import (
    Any,
    Callable,
    Mapping,
    MutableMapping,
    Optional,
    Sequence,
)

from confluent_kafka import OFFSET_BEGINNING, OFFSET_END, OFFSET_INVALID, OFFSET_STORED
from confluent_kafka import Consumer as ConfluentConsumer
from confluent_kafka import KafkaError
from confluent_kafka import Message as ConfluentMessage
from confluent_kafka import Producer as ConfluentProducer
from confluent_kafka import TopicPartition as ConfluentTopicPartition

from snuba.utils.retries import NoRetryPolicy, RetryPolicy


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Topic:
    __slots__ = ["name"]

    name: str

    def __contains__(self, partition: Partition) -> bool:
        return partition.topic == self


@dataclass(frozen=True)
class Partition:
    __slots__ = ["topic", "index"]

    topic: Topic
    index: int


class ConsumerError(Exception):
    """
    Base class for exceptions that are raised during consumption.

    Subclasses may extend this class to disambiguate errors that are specific
    to their implementation.
    """


class EndOfPartition(ConsumerError):
    """
    Raised when there are no more messages to consume from the partition.
    """

    def __init__(self, partition: Partition, offset: int):
        # The partition that the consumer has reached the end of.
        self.partition = partition

        # The next unconsumed offset in the partition (where there is currently
        # no message.)
        self.offset = offset


class TransportError(ConsumerError):
    pass


KafkaConsumerState = Enum(
    "KafkaConsumerState", ["CONSUMING", "ERROR", "CLOSED", "ASSIGNING", "REVOKING"]
)


class InvalidState(RuntimeError):
    def __init__(self, state: KafkaConsumerState):
        self.__state = state


@dataclass(frozen=True)
class KafkaMessage:
    """
    Represents a single message within a partition.
    """

    __slots__ = ["partition", "offset", "value"]

    partition: Partition
    offset: int
    value: bytes

    def get_next_offset(self) -> int:
        return self.offset + 1


class KafkaConsumer:
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

        # NOTE: Offsets are explicitly managed as part of the assignment
        # callback, so preemptively resetting offsets is not enabled.
        self.__consumer = ConfluentConsumer(
            {**configuration, "auto.offset.reset": "error"}
        )

        self.__offsets: MutableMapping[Partition, int] = {}

        self.__commit_retry_policy = commit_retry_policy

        self.__state = KafkaConsumerState.CONSUMING

    def __resolve_partition_offset_earliest(
        self, partition: ConfluentTopicPartition
    ) -> int:
        low, high = self.__consumer.get_watermark_offsets(partition)
        return int(low)

    def __resolve_partition_offset_latest(
        self, partition: ConfluentTopicPartition
    ) -> int:
        low, high = self.__consumer.get_watermark_offsets(partition)
        return int(high)

    def __resolve_partition_offset_error(
        self, partition: ConfluentTopicPartition
    ) -> ConfluentTopicPartition:
        raise ConsumerError("unable to resolve partition offsets")

    def __get_starting_offsets(
        self, partitions: Sequence[Partition]
    ) -> Mapping[Partition, int]:
        result: MutableMapping[Partition, int] = {}

        for item in self.__consumer.committed(
            [
                ConfluentTopicPartition(partition.topic.name, partition.index)
                for partition in partitions
            ]
        ):
            partition = Partition(Topic(item.topic), item.partition)
            if item.offset >= 0:
                result[partition] = item.offset
            elif item.offset == OFFSET_INVALID:
                result[partition] = self.__resolve_partition_starting_offset(item)
            else:
                raise ValueError("received unexpected offset")

        return result

    # Balanced Consumer Methods

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
                offsets = self.__get_starting_offsets(
                    [
                        Partition(Topic(partition.topic), partition.partition)
                        for partition in partitions
                    ]
                )
                self.__seek(offsets)
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

    # Managed Consumer Methods

    def assign(
        self, partitions: Mapping[Partition, Optional[int]]
    ) -> Mapping[Partition, int]:
        """
        Set the current partition assignment. This replaces any existing
        assignment.

        If no offset is provided, the last committed offset is used. If there
        is no committed offset stored, the strategy specified by the
        ``auto.offset.reset`` configuration parameter is used to determine
        the starting offset.

        Raises an ``InvalidState`` exception if called on a closed consumer.
        """
        if self.__state is not KafkaConsumerState.CONSUMING:
            raise InvalidState(self.__state)

        assignment = {
            **{
                partition: offset
                for partition, offset in partitions.items()
                if offset is not None
            },
            **self.__get_starting_offsets(
                [
                    partition
                    for partition, offset in partitions.items()
                    if offset is None
                ]
            ),
        }

        self.__consumer.assign(
            [
                ConfluentTopicPartition(partition.topic.name, partition.index, offset)
                for partition, offset in assignment.items()
            ]
        )

        self.__offsets = assignment

        return assignment

    def unassign(self) -> None:
        """
        Remove the current assignment.

        Raises an ``InvalidState`` exception if called on a closed consumer.
        """
        if self.__state is not KafkaConsumerState.CONSUMING:
            raise InvalidState(self.__state)

        self.__consumer.unassign()
        self.__offsets.clear()

    def poll(self, timeout: Optional[float] = None) -> Optional[KafkaMessage]:
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
            else:
                raise ConsumerError(str(error))

        result = KafkaMessage(
            Partition(Topic(message.topic()), message.partition()),
            message.offset(),
            message.value(),
        )

        self.__offsets[result.partition] = result.get_next_offset()

        return result

    def tell(self) -> Mapping[Partition, int]:
        """
        Return the read offsets for all assigned partitions.

        Raises an ``InvalidState`` if called on a closed consumer.
        """
        if self.__state in {KafkaConsumerState.CLOSED, KafkaConsumerState.ERROR}:
            raise InvalidState(self.__state)

        return self.__offsets

    def __seek(self, offsets: Mapping[Partition, int]) -> None:
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

        self.__consumer.pause(
            [
                ConfluentTopicPartition(partition.topic.name, partition.index)
                for partition in partitions
            ]
        )

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

        self.__consumer.resume(
            [
                ConfluentTopicPartition(partition.topic.name, partition.index)
                for partition in partitions
            ]
        )

    def __commit(self) -> Mapping[Partition, int]:
        if self.__state in {KafkaConsumerState.CLOSED, KafkaConsumerState.ERROR}:
            raise InvalidState(self.__state)

        result: Optional[Sequence[ConfluentTopicPartition]] = self.__consumer.commit(
            asynchronous=False
        )
        assert result is not None  # synchronous commit should return result immediately

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

    def commit(self) -> Mapping[Partition, int]:
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


DEFAULT_QUEUED_MAX_MESSAGE_KBYTES = 50000
DEFAULT_QUEUED_MIN_MESSAGES = 10000


def build_kafka_consumer_configuration(
    bootstrap_servers: Sequence[str],
    group_id: str,
    auto_offset_reset: str = "error",
    queued_max_messages_kbytes: int = DEFAULT_QUEUED_MAX_MESSAGE_KBYTES,
    queued_min_messages: int = DEFAULT_QUEUED_MIN_MESSAGES,
) -> Mapping[str, Any]:
    return {
        "enable.auto.commit": False,
        "bootstrap.servers": ",".join(bootstrap_servers),
        "group.id": group_id,
        "auto.offset.reset": auto_offset_reset,
        # overridden to reduce memory usage when there's a large backlog
        "queued.max.messages.kbytes": queued_max_messages_kbytes,
        "queued.min.messages": queued_min_messages,
        "enable.partition.eof": False,
    }


class KafkaConsumerWithCommitLog(KafkaConsumer):
    def __init__(
        self,
        configuration: Mapping[str, Any],
        *,
        producer: ConfluentProducer,
        commit_log_topic: Topic,
        commit_retry_policy: Optional[RetryPolicy] = None,
    ) -> None:
        super().__init__(configuration, commit_retry_policy=commit_retry_policy)
        self.__producer = producer
        self.__commit_log_topic = commit_log_topic
        self.__group_id = configuration["group.id"]

    def poll(self, timeout: Optional[float] = None) -> Optional[KafkaMessage]:
        self.__producer.poll(0.0)
        return super().poll(timeout)

    def __commit_message_delivery_callback(
        self, error: Optional[KafkaError], message: ConfluentMessage
    ) -> None:
        if error is not None:
            raise Exception(error.str())

    def commit(self) -> Mapping[Partition, int]:
        offsets = super().commit()

        for partition, offset in offsets.items():
            self.__producer.produce(
                self.__commit_log_topic.name,
                key="{}:{}:{}".format(
                    partition.topic.name, partition.index, self.__group_id
                ).encode("utf-8"),
                value="{}".format(offset).encode("utf-8"),
                on_delivery=self.__commit_message_delivery_callback,
            )

        return offsets

    def close(self, timeout: Optional[float] = None) -> None:
        super().close()
        messages: int = self.__producer.flush(*[timeout] if timeout is not None else [])
        if messages > 0:
            raise TimeoutError(f"{messages} commit log messages pending delivery")
