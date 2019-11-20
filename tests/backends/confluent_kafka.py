from typing import Optional
from unittest.mock import MagicMock

from confluent_kafka import TopicPartition, KafkaError
from confluent_kafka.admin import (
    ClusterMetadata,
    PartitionMetadata,
    TopicMetadata,
)


class FakeConfluentKafkaMessage(object):
    def __init__(
        self,
        topic: str,
        partition: int,
        offset,
        value: Optional[bytes],
        key=None,
        headers=None,
        error=None,
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

    def topic(self):
        return self._topic

    def partition(self):
        return self._partition

    def offset(self):
        return self._offset

    def value(self) -> Optional[bytes]:
        return self._value

    def key(self):
        return self._key

    def headers(self):
        return self._headers

    def error(self):
        return self._error


class FakeConfluentKafkaProducer(object):
    def __init__(self):
        self.messages = []
        self._callbacks = []

    def poll(self, *args, **kwargs):
        while self._callbacks:
            callback, message = self._callbacks.pop()
            callback(None, message)
        return 0

    def flush(self):
        return self.poll()

    def produce(self, topic, value, key=None, headers=None, on_delivery=None):
        message = FakeConfluentKafkaMessage(
            topic=topic,
            partition=None,  # XXX: the partition is unknown (depends on librdkafka)
            offset=None,  # XXX: the offset is unknown (depends on state)
            key=key,
            value=value,
            headers=headers,
        )
        self.messages.append(message)
        if on_delivery is not None:
            self._callbacks.append((on_delivery, message))


class FakeConfluentKafkaConsumer(object):
    def __init__(self):
        self.items = []
        self.commit_calls = 0
        self.close_calls = 0
        self.positions = {}

    def poll(self, *args, **kwargs):
        try:
            message = self.items.pop(0)
        except IndexError:
            return None

        self.positions[(message.topic(), message.partition())] = message.offset() + 1

        return message

    def commit(self, *args, **kwargs):
        self.commit_calls += 1
        return [
            TopicPartition(topic, partition, offset)
            for (topic, partition), offset in self.positions.items()
        ]

    def close(self, *args, **kwargs):
        self.close_calls += 1

    def subscribe(self, *args, **kwargs):
        pass

    def list_topics(self, topic):
        meta = ClusterMetadata()
        topic_meta = TopicMetadata()
        topic_meta.topic = topic
        topic_meta.partitions = {0: PartitionMetadata()}
        meta.topics = {topic: topic_meta}
        return meta


def build_confluent_kafka_message(
    offset: int, partition: int, value: Optional[bytes], eof: bool = False
) -> FakeConfluentKafkaMessage:
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
        value=value,
        key=None,
        headers=None,
        error=error,
    )
