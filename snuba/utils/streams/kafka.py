import logging
import time
from typing import Any, Callable, Mapping, NamedTuple, Optional, Sequence

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
    def __init__(self, configuration: Mapping[str, Any]) -> None:
        self.configuration = configuration  # XXX: Reconsider this?
        self.__consumer = ConfluentConsumer(configuration)

    def __wrap_assignment_callback(
        self, callback: Callable[[Sequence[TopicPartition]], None]
    ) -> Callable[[ConfluentConsumer, Sequence[ConfluentTopicPartition]], None]:
        def wrapper(
            consumer: ConfluentConsumer, partitions: Sequence[ConfluentTopicPartition]
        ) -> None:
            callback(
                [TopicPartition(value.topic, value.partition) for value in partitions]
            )

        return wrapper

    def subscribe(
        self,
        topics: Sequence[str],
        on_assign: Optional[Callable[[Sequence[TopicPartition]], None]] = None,
        on_revoke: Optional[Callable[[Sequence[TopicPartition]], None]] = None,
    ) -> None:
        kwargs = {}

        if on_assign is not None:
            kwargs["on_assign"] = self.__wrap_assignment_callback(on_assign)

        if on_revoke is not None:
            kwargs["on_revoke"] = self.__wrap_assignment_callback(on_revoke)

        self.__consumer.subscribe(topics, **kwargs)

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
                raise EndOfStream(TopicPartition(message.topic(), message.partition()))
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

        return {
            TopicPartition(value.topic, value.partition): value.offset
            for value in result
        }

    def close(self) -> None:
        self.__consumer.close()


DEFAULT_QUEUED_MAX_MESSAGE_KBYTES = 50000
DEFAULT_QUEUED_MIN_MESSAGES = 10000


def build_kafka_consumer(
    bootstrap_servers: Sequence[str],
    group_id: str,
    auto_offset_reset: str = "error",
    queued_max_messages_kbytes: int = DEFAULT_QUEUED_MAX_MESSAGE_KBYTES,
    queued_min_messages: int = DEFAULT_QUEUED_MIN_MESSAGES,
) -> KafkaConsumer:
    return KafkaConsumer(
        {
            "enable.auto.commit": False,
            "bootstrap.servers": ",".join(bootstrap_servers),
            "group.id": group_id,
            "default.topic.config": {"auto.offset.reset": auto_offset_reset},
            # overridden to reduce memory usage when there's a large backlog
            "queued.max.messages.kbytes": queued_max_messages_kbytes,
            "queued.min.messages": queued_min_messages,
            "enable.partition.eof": False,
        }
    )


class KafkaConsumerWithCommitLog(Consumer[TopicPartition, int, bytes]):

    # Set of logical (not literal) offsets to not publish to the commit log.
    # https://github.com/confluentinc/confluent-kafka-python/blob/443177e1c83d9b66ce30f5eb8775e062453a738b/tests/test_enums.py#L22-L25
    LOGICAL_OFFSETS = frozenset(
        [OFFSET_BEGINNING, OFFSET_END, OFFSET_STORED, OFFSET_INVALID]
    )

    def __init__(
        self,
        consumer: KafkaConsumer,
        producer: ConfluentProducer,
        commit_log_topic: str,
    ) -> None:
        self.__consumer = consumer
        self.__producer = producer
        self.__commit_log_topic = commit_log_topic

    def subscribe(
        self,
        topics: Sequence[str],
        on_assign: Optional[Callable[[Sequence[TopicPartition]], None]] = None,
        on_revoke: Optional[Callable[[Sequence[TopicPartition]], None]] = None,
    ) -> None:
        self.__consumer.subscribe(topics, on_assign, on_revoke)

    def poll(self, timeout: Optional[float] = None) -> Optional[KafkaMessage]:
        self.__producer.poll(0.0)
        return self.__consumer.poll(timeout)

    def __commit_message_delivery_callback(
        self, error: Optional[KafkaError], message: ConfluentMessage
    ) -> None:
        if error is not None:
            raise Exception(error.str())

    def commit(self) -> Mapping[TopicPartition, int]:
        offsets = self.__consumer.commit()

        if self.__commit_log_topic:
            for stream, offset in offsets.items():
                # XXX: This is an abstraction leak from the Kafka consumer.
                if offset in self.LOGICAL_OFFSETS:
                    logger.debug(
                        "Skipped publishing logical offset (%r) to commit log for %s",
                        offset,
                        stream,
                    )
                    continue
                elif offset < 0:
                    logger.warning(
                        "Found unexpected negative offset (%r) after commit for %s",
                        offset,
                        stream,
                    )

                self.__producer.produce(
                    self.__commit_log_topic,
                    key="{}:{}:{}".format(
                        stream.topic,
                        stream.partition,
                        self.__consumer.configuration["group_id"],
                    ).encode("utf-8"),
                    value="{}".format(offset).encode("utf-8"),
                    on_delivery=self.__commit_message_delivery_callback,
                )

        return offsets

    def close(self) -> None:
        self.__producer.flush()
        self.__consumer.close()
