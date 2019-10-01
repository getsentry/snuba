import time
from datetime import datetime
from typing import (
    Callable,
    Mapping,
    MutableMapping,
    MutableSequence,
    Optional,
    Sequence,
    Tuple,
)

from mock import patch

from snuba.utils.kafka.consumers.abstract import Consumer, Offset, TopicPartitionKey
from snuba.utils.kafka.consumers.batching import (
    AbstractBatchWorker,
    BatchingKafkaConsumer,
)


class FakeKafkaMessage:
    def __init__(
        self,
        topic: str,
        partition: int,
        offset: int,
        value: str,
        key: str = None,
        headers=None,
        error=None,
    ) -> None:
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

    def value(self) -> str:
        return self._value

    def key(self) -> str:
        return self._key

    def headers(self):
        return self._headers

    def error(self):
        return self._error


class FakeKafkaProducer(object):
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
        message = FakeKafkaMessage(
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


class FakeKafkaConsumer(Consumer[FakeKafkaMessage]):
    def __init__(self) -> None:
        self.items: MutableSequence[FakeKafkaMessage] = []
        self.commit_calls = 0
        self.close_calls = 0
        self.positions: MutableMapping[Tuple[str, int], Offset] = {}

    def subscribe(
        self,
        topics: Sequence[str],
        on_assign: Optional[Callable[[Sequence[TopicPartitionKey]], None]] = None,
        on_revoke: Optional[Callable[[Sequence[TopicPartitionKey]], None]] = None,
    ) -> None:
        raise NotImplementedError

    def poll(self, timeout: Optional[float] = None) -> Optional[FakeKafkaMessage]:
        try:
            message = self.items.pop(0)
        except IndexError:
            return None

        self.positions[
            TopicPartitionKey(message.topic(), message.partition())
        ] = Offset(message.offset() + 1)

        return message

    def commit(
        self, asynchronous: bool = True
    ) -> Optional[Mapping[TopicPartitionKey, Offset]]:
        self.commit_calls += 1
        return self.positions

    def close(self) -> None:
        self.close_calls += 1


class FakeBatchingKafkaConsumer(BatchingKafkaConsumer[FakeKafkaMessage]):
    def create_consumer(self, *args, **kwargs) -> FakeKafkaConsumer:
        return FakeKafkaConsumer()


class FakeWorker(AbstractBatchWorker):
    def __init__(self, *args, **kwargs):
        super(FakeWorker, self).__init__(*args, **kwargs)
        self.processed = []
        self.flushed = []
        self.shutdown_calls = 0

    def process_message(self, message):
        self.processed.append(message.value())
        return message.value()

    def flush_batch(self, batch):
        self.flushed.append(batch)

    def shutdown(self):
        self.shutdown_calls += 1


class TestConsumer(object):
    def test_batch_size(self):
        consumer = FakeBatchingKafkaConsumer(
            "topic",
            worker=FakeWorker(),
            max_batch_size=2,
            max_batch_time=100,
            bootstrap_servers=None,
            group_id="group",
            commit_log_topic="commits",
            producer=FakeKafkaProducer(),
        )

        consumer.consumer.items = [
            FakeKafkaMessage("topic", 0, i, i) for i in [1, 2, 3]
        ]
        for x in range(len(consumer.consumer.items)):
            consumer._run_once()
        consumer._shutdown()

        assert consumer.worker.processed == [1, 2, 3]
        assert consumer.worker.flushed == [[1, 2]]
        assert consumer.worker.shutdown_calls == 1
        assert consumer.consumer.commit_calls == 1
        assert consumer.consumer.close_calls == 1

        assert len(consumer.producer.messages) == 1
        commit_message = consumer.producer.messages[0]
        assert commit_message.topic() == "commits"
        assert commit_message.key() == "{}:{}:{}".format("topic", 0, "group").encode(
            "utf-8"
        )
        assert commit_message.value() == "{}".format(2 + 1).encode(
            "utf-8"
        )  # offsets are last processed message offset + 1

    @patch("time.time")
    def test_batch_time(self, mock_time):
        consumer = FakeBatchingKafkaConsumer(
            "topic",
            worker=FakeWorker(),
            max_batch_size=100,
            max_batch_time=2000,
            bootstrap_servers=None,
            group_id="group",
            commit_log_topic="commits",
            producer=FakeKafkaProducer(),
        )

        mock_time.return_value = time.mktime(datetime(2018, 1, 1, 0, 0, 0).timetuple())
        consumer.consumer.items = [
            FakeKafkaMessage("topic", 0, i, i) for i in [1, 2, 3]
        ]
        for x in range(len(consumer.consumer.items)):
            consumer._run_once()

        mock_time.return_value = time.mktime(datetime(2018, 1, 1, 0, 0, 1).timetuple())
        consumer.consumer.items = [
            FakeKafkaMessage("topic", 0, i, i) for i in [4, 5, 6]
        ]
        for x in range(len(consumer.consumer.items)):
            consumer._run_once()

        mock_time.return_value = time.mktime(datetime(2018, 1, 1, 0, 0, 5).timetuple())
        consumer.consumer.items = [
            FakeKafkaMessage("topic", 0, i, i) for i in [7, 8, 9]
        ]
        for x in range(len(consumer.consumer.items)):
            consumer._run_once()

        consumer._shutdown()

        assert consumer.worker.processed == [1, 2, 3, 4, 5, 6, 7, 8, 9]
        assert consumer.worker.flushed == [[1, 2, 3, 4, 5, 6]]
        assert consumer.worker.shutdown_calls == 1
        assert consumer.consumer.commit_calls == 1
        assert consumer.consumer.close_calls == 1

        assert len(consumer.producer.messages) == 1
        commit_message = consumer.producer.messages[0]
        assert commit_message.topic() == "commits"
        assert commit_message.key() == "{}:{}:{}".format("topic", 0, "group").encode(
            "utf-8"
        )
        assert commit_message.value() == "{}".format(6 + 1).encode(
            "utf-8"
        )  # offsets are last processed message offset + 1

    def test_dead_letter_topic(self):
        class FailingFakeWorker(FakeWorker):
            def process_message(*args, **kwargs):
                1 / 0

        producer = FakeKafkaProducer()
        consumer = FakeBatchingKafkaConsumer(
            "topic",
            worker=FailingFakeWorker(),
            max_batch_size=100,
            max_batch_time=2000,
            bootstrap_servers=None,
            group_id="group",
            producer=producer,
            dead_letter_topic="dlt",
        )

        message = FakeKafkaMessage(
            "topic", partition=1, offset=2, key="key", value="value"
        )
        consumer.consumer.items = [message]
        consumer._run_once()

        assert len(producer.messages) == 1
        produced_message = producer.messages[0]

        assert ("dlt", message.key(), message.value()) == (
            produced_message.topic(),
            produced_message.key(),
            produced_message.value(),
        )

        assert produced_message.headers() == {
            "partition": "1",
            "offset": "2",
            "topic": "topic",
        }
