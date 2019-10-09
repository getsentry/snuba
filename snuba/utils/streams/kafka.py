from typing import Any, Callable, Mapping, NamedTuple, Optional, Sequence

from confluent_kafka import Consumer as ConfluentConsumer
from confluent_kafka import KafkaError
from confluent_kafka import Message as ConfluentMessage
from confluent_kafka import TopicPartition as ConfluentTopicPartition

from snuba.utils.streams.abstract import Consumer, ConsumerError, EndOfStream, Message


class TopicPartition(NamedTuple):
    topic: str
    partition: int


KafkaMessage = Message[TopicPartition, int, bytes]


class KafkaConsumer(Consumer[TopicPartition, int, bytes]):
    def __init__(self, configuration: Mapping[str, Any]) -> None:
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
            if error.code() == KafkaError._PARTITION_EOF:
                raise EndOfStream(TopicPartition(message.topic(), message.partition()))
            else:
                raise ConsumerError  # TODO: Be more specific about the error raised here.

        return KafkaMessage(
            TopicPartition(message.topic(), message.partition()),
            message.offset(),
            message.value(),
        )

    def commit(self) -> Mapping[TopicPartition, int]:
        result: Sequence[ConfluentTopicPartition] = self.__consumer.commit(
            asynchronous=False
        )
        return {
            TopicPartition(value.topic, value.partition): value.offset
            for value in result
        }

    def close(self) -> None:
        self.__consumer.close()


DEFAULT_QUEUED_MAX_MESSAGE_KBYTES = 50000
DEFAULT_QUEUED_MIN_MESSAGES = 10000


def build_confluent_kafka_consumer(
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
        }
    )
