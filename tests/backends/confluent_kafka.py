from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple
from unittest.mock import MagicMock

from confluent_kafka import KafkaError, TopicPartition
from confluent_kafka.admin import ClusterMetadata, PartitionMetadata, TopicMetadata


class FakeConfluentKafkaMessage(object):
    def __init__(
        self,
        topic: str,
        partition: int,
        offset: int,
        value: bytes,
        key: Any = None,
        headers: Any = None,
        error: Any = None,
    ) -> None:
        if value is not None:
            assert isinstance(value, bytes)

        self._topic = topic
        self._partition = partition
        self._offset = offset
        self._value = value
        self._key = key
        self._headers = (
            {str(k): str(v) if v else None for k, v in headers.items()}
            if headers
            else None
        )
        self._headers = headers
        self._error = error

    def topic(self) -> str:
        return self._topic

    def partition(self) -> int:
        return self._partition

    def offset(self) -> int:
        return self._offset

    def value(self) -> Optional[bytes]:
        return self._value

    def key(self) -> Any:
        return self._key

    def headers(self) -> Any:
        return self._headers

    def error(self) -> Any:
        return self._error


DeliveryCallback = Callable[[Optional[Exception], FakeConfluentKafkaMessage], None]


class FakeConfluentKafkaProducer(object):
    def __init__(self) -> None:
        self.messages: List[FakeConfluentKafkaMessage] = []
        self._callbacks: List[Tuple[DeliveryCallback, FakeConfluentKafkaMessage]] = []

    def poll(self, *args: Any, **kwargs: Any) -> int:
        while self._callbacks:
            callback, message = self._callbacks.pop()
            callback(None, message)
        return 0

    def flush(self) -> int:
        return self.poll()

    def produce(
        self,
        topic: str,
        value: bytes,
        key: Any = None,
        headers: Any = None,
        on_delivery: Optional[DeliveryCallback] = None,
    ) -> None:
        message = FakeConfluentKafkaMessage(
            topic=topic,
            partition=0,  # XXX: the partition is unknown (depends on librdkafka)
            offset=0,  # XXX: the offset is unknown (depends on state)
            key=key,
            value=value,
            headers=headers,
        )
        self.messages.append(message)
        if on_delivery is not None:
            self._callbacks.append((on_delivery, message))


class FakeConfluentKafkaConsumer(object):
    def __init__(self) -> None:
        self.items: List[FakeConfluentKafkaMessage] = []
        self.commit_calls = 0
        self.close_calls = 0
        self.positions: Dict[Tuple[str, Optional[int]], Optional[int]] = {}

    def poll(self, *args: Any, **kwargs: Any) -> Optional[FakeConfluentKafkaMessage]:
        try:
            message = self.items.pop(0)
        except IndexError:
            return None

        self.positions[(message.topic(), message.partition())] = message.offset() + 1

        return message

    def commit(self, *args: Any, **kwargs: Any) -> Sequence[TopicPartition]:
        self.commit_calls += 1
        return [
            TopicPartition(topic, partition, offset)
            for (topic, partition), offset in self.positions.items()
        ]

    def close(self, *args: Any, **kwargs: Any) -> None:
        self.close_calls += 1

    def subscribe(self, *args: Any, **kwargs: Any) -> None:
        pass

    def list_topics(self, topic: str) -> ClusterMetadata:
        meta = ClusterMetadata()
        topic_meta = TopicMetadata()
        topic_meta.topic = topic
        topic_meta.partitions = {0: PartitionMetadata()}
        meta.topics = {topic: topic_meta}
        return meta


def build_confluent_kafka_message(
    offset: int, partition: int, value: Optional[bytes], eof: bool = False
) -> FakeConfluentKafkaMessage:
    error: Optional[Any]
    if eof:
        error = MagicMock()
        error.code.return_value = KafkaError._PARTITION_EOF
        assert value is None
    else:
        error = None

    return FakeConfluentKafkaMessage(
        topic="topic",
        partition=partition,
        offset=offset,
        value=value or b"",
        key=None,
        headers=None,
        error=error,
    )
