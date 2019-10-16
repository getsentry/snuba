import logging
import time
from typing import (
    Any,
    Callable,
    Mapping,
    MutableMapping,
    MutableSequence,
    NamedTuple,
    Optional,
    Sequence,
)

from confluent_kafka import OFFSET_BEGINNING, OFFSET_END, OFFSET_INVALID, OFFSET_STORED
from confluent_kafka import Consumer as ConfluentConsumer
from confluent_kafka import KafkaError, KafkaException
from confluent_kafka import Message as ConfluentMessage
from confluent_kafka import Producer as ConfluentProducer
from confluent_kafka import TopicPartition as ConfluentTopicPartition

from snuba.utils.streams.abstract import Consumer, ConsumerError, EndOfStream, Message

logger = logging.getLogger(__name__)


class TopicPartition(NamedTuple):
    topic: str
    partition: int


KafkaMessage = Message[TopicPartition, int, bytes]


class TransportError(ConsumerError):
    pass


class KafkaConsumer(Consumer[TopicPartition, int, bytes]):
    """
    The behavior of the of this consumer differs slightly from the Confluent
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
    Confluent consumer as well: offsets are only reset during the assignment
    callback. Any other circumstances that would otherwise lead to preemptive
    offset reset (e.g. the consumer tries to read a message that is before
    the earliest offset, or the consumer attempts to read a message that is
    after the latest offset) will cause an exception to be thrown, rather
    than resetting the offset (which could lead to chunks messages being
    replayed or skipped, depending on the circumstances.)
    """

    # Set of logical offsets that do not correspond to actual log positions.
    # These offsets should be considered an implementation detail of the Kafka
    # consumer and not used publically.
    # https://github.com/confluentinc/confluent-kafka-python/blob/443177e1c83d9b66ce30f5eb8775e062453a738b/tests/test_enums.py#L22-L25
    LOGICAL_OFFSETS = frozenset(
        [OFFSET_BEGINNING, OFFSET_END, OFFSET_STORED, OFFSET_INVALID]
    )

    def __init__(self, configuration: Mapping[str, Any]) -> None:
        auto_offset_reset = configuration.get("auto.offset.reset", "largest")
        if auto_offset_reset in {"smallest", "earliest", "beginning"}:
            self.__resolve_partition_starting_offset = self.__resolve_partition_offset_earliest
        elif auto_offset_reset in {"largest", "latest", "end"}:
            self.__resolve_partition_starting_offset = self.__resolve_partition_offset_latest
        elif auto_offset_reset == "error":
            self.__resolve_partition_starting_offset = self.__resolve_partition_offset_error
        else:
            raise ValueError("invalid value for 'auto.offset.reset' configuration")

        self.__consumer = ConfluentConsumer(
            {**configuration, "auto.offset.reset": "error"}
        )

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
        raise ConsumerError

    def subscribe(
        self,
        topics: Sequence[str],
        on_assign: Optional[Callable[[Sequence[TopicPartition]], None]] = None,
        on_revoke: Optional[Callable[[Sequence[TopicPartition]], None]] = None,
    ) -> None:
        def assignment_callback(
            consumer: ConfluentConsumer, partitions: Sequence[ConfluentTopicPartition]
        ) -> None:
            assignment: MutableSequence[ConfluentTopicPartition] = []

            for partition in self.__consumer.committed(partitions):
                if partition.offset >= 0:
                    assignment.append(partition)
                elif partition.offset == OFFSET_INVALID:
                    assignment.append(self.__resolve_partition_starting_offset(partition))
                else:
                    raise ValueError("received unexpected offset")

            self.__consumer.assign(assignment)

            if on_assign is not None:
                on_assign([TopicPartition(i.topic, i.partition) for i in partitions])

        def revocation_callback(
            consumer: ConfluentConsumer, partitions: Sequence[ConfluentTopicPartition]
        ) -> None:
            if on_revoke is not None:
                on_revoke([TopicPartition(i.topic, i.partition) for i in partitions])

        self.__consumer.subscribe(
            topics, on_assign=assignment_callback, on_revoke=revocation_callback
        )

    def unsubscribe(self) -> None:
        self.__consumer.unsubscribe()

    def poll(self, timeout: Optional[float] = None) -> Optional[KafkaMessage]:
        message: Optional[ConfluentMessage] = self.__consumer.poll(
            *[timeout] if timeout is not None else []
        )
        if message is None:
            return None

        error: Optional[KafkaError] = message.error()
        if error is not None:
            code = error.code()
            if code == KafkaError._PARTITION_EOF:
                raise EndOfStream(
                    TopicPartition(message.topic(), message.partition()),
                    message.offset(),
                )
            elif code == KafkaError._TRANSPORT:
                raise TransportError(str(error))
            else:
                raise ConsumerError(str(error))

        return KafkaMessage(
            TopicPartition(message.topic(), message.partition()),
            message.offset(),
            message.value(),
        )

    def commit(self) -> Mapping[TopicPartition, int]:
        result: Optional[Sequence[ConfluentTopicPartition]] = None

        retries_remaining = 3
        while result is None:
            try:
                result = self.__consumer.commit(asynchronous=False)
                assert result is not None
            except KafkaException as e:
                if not e.args[0].code() in (
                    KafkaError.REQUEST_TIMED_OUT,
                    KafkaError.NOT_COORDINATOR_FOR_GROUP,
                    KafkaError._WAIT_COORD,
                ):
                    raise

                if not retries_remaining:
                    raise

                logger.warning(
                    "Commit failed: %s (%d retries remaining)",
                    str(e),
                    retries_remaining,
                )
                retries_remaining -= 1
                time.sleep(1)

        offsets: MutableMapping[TopicPartition, int] = {}

        for value in result:
            # The Confluent Kafka Consumer will include logical offsets in the
            # sequence of ``TopicPartition`` objects returned by ``commit``.
            # These are an implementation detail of the Kafka Consumer, so we
            # don't expose them here.
            # NOTE: These should no longer be seen now that we are forcing
            # offsets to be set as part of the assignment callback.
            if value.offset in self.LOGICAL_OFFSETS:
                continue

            assert value.offset >= 0, "expected non-negative offset"
            offsets[TopicPartition(value.topic, value.partition)] = value.offset

        return offsets

    def close(self, timeout: Optional[float] = None) -> None:
        try:
            self.__consumer.close()
        except RuntimeError:
            pass


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
        producer: ConfluentProducer,
        commit_log_topic: str,
    ) -> None:
        super().__init__(configuration)
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

    def commit(self) -> Mapping[TopicPartition, int]:
        offsets = super().commit()

        for stream, offset in offsets.items():
            self.__producer.produce(
                self.__commit_log_topic,
                key="{}:{}:{}".format(
                    stream.topic, stream.partition, self.__group_id
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
