import logging
import time
from typing import Any, Callable, Mapping, NamedTuple, Optional, Sequence

from confluent_kafka import OFFSET_INVALID
from confluent_kafka import Consumer as ConfluentConsumer
from confluent_kafka import KafkaError, KafkaException
from confluent_kafka import Message as ConfluentMessage
from confluent_kafka import TopicPartition as ConfluentTopicPartition

from snuba.utils.streams.abstract import (
    Consumer,
    ConsumerError,
    EndOfStream,
    Message,
    StreamPosition,
)


logger = logging.getLogger(__name__)


class TopicPartition(NamedTuple):
    topic: str
    partition: int


KafkaMessage = Message[TopicPartition, int, bytes]


class TransportError(ConsumerError):
    pass


class KafkaConsumer(Consumer[TopicPartition, int, bytes]):
    def __init__(self, configuration: Mapping[str, Any]) -> None:
        self.__consumer = ConfluentConsumer(configuration)
        self.__auto_offset_reset = configuration[
            "auto.offset.reset"
        ]  # TODO: This needs a default, aliasing, etc.
        self.__offsets = {}

    def subscribe(
        self,
        topics: Sequence[str],
        on_assign: Optional[Callable[[Sequence[TopicPartition]], None]] = None,
        on_revoke: Optional[Callable[[Sequence[TopicPartition]], None]] = None,
    ) -> None:
        def assignment_callback(consumer, partitions):
            offsets = {}

            for i in self.__consumer.committed(partitions):
                stream = TopicPartition(i.topic, i.partition)
                assert stream not in self.__offsets

                if i.offset > -1:
                    offsets[stream] = i.offset
                elif i.offset == OFFSET_INVALID:
                    low, high = self.__consumer.get_watermark_offsets(
                        i
                    )  # TODO: Arguments?
                    if self.__auto_offset_reset == "earliest":
                        offsets[stream] = low
                    elif self.__auto_offset_reset == "latest":
                        offsets[stream] = high
                    else:
                        raise NotImplementedError  # TODO
                else:
                    raise Exception("Unexpected offset received")

            self.__offsets.update(offsets)

            self.__consumer.assign(
                [
                    ConfluentTopicPartition(stream.topic, stream.partition, offset)
                    for stream, offset in self.__offsets.items()
                ]
            )

            if on_assign is not None:
                on_assign(list(offsets.keys()))

        def revocation_callback(consumer, partitions):
            streams = [
                TopicPartition(value.topic, value.partition) for value in partitions
            ]

            try:
                if on_revoke is not None:
                    on_revoke(streams)
            finally:
                # Finish cleaning up.
                for stream in streams:
                    del self.__offsets[stream]

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
                raise EndOfStream(TopicPartition(message.topic(), message.partition()))
            elif code == KafkaError._TRANSPORT:
                raise TransportError(str(error))
            else:
                raise ConsumerError(str(error))

        stream = TopicPartition(message.topic(), message.partition())
        self.__offsets[stream] = message.offset() + 1

        return KafkaMessage(stream, message.offset(), message.value())

    def tell(self) -> Mapping[TopicPartition, int]:
        return self.__offsets

    def seek(
        self,
        offsets: Mapping[TopicPartition, int],
        whence: StreamPosition = StreamPosition.SET,
    ) -> Mapping[TopicPartition, int]:
        # TODO: This method needs to use ``assign`` instead of ``seek`` if
        # called from an assignment callback. (What happens if the offsets only
        # affect a subset of the assignment?)
        # TODO: How should this method deal with calls during the revocation
        # callback?
        assert StreamPosition.SET  # TODO
        for stream, offset in offsets.items():
            self.__offsets[stream] = offset
            self.__consumer.seek(
                ConfluentTopicPartition(stream.topic, stream.partition, offset)
            )
        return offsets

    def commit_offsets(self) -> Mapping[TopicPartition, int]:
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
