import time
from datetime import datetime
from typing import (
    Any,
    MutableSequence,
    Sequence,
    Optional,
)
from unittest.mock import patch

from snuba.utils.metrics.backends.dummy import DummyMetricsBackend
from snuba.utils.streams.batching import AbstractBatchWorker, BatchingConsumer
from snuba.utils.streams.dummy import DummyConsumer
from snuba.utils.streams.kafka import KafkaPayload
from snuba.utils.streams.types import Message, Partition, Topic


class FakeWorker(AbstractBatchWorker[KafkaPayload, bytes]):
    def __init__(self) -> None:
        self.processed: MutableSequence[Optional[Any]] = []
        self.flushed: MutableSequence[Sequence[Any]] = []

    def process_message(self, message: Message[KafkaPayload]) -> bytes:
        self.processed.append(message.payload.value)
        return message.payload.value

    def flush_batch(self, batch: Sequence[Any]) -> None:
        self.flushed.append(batch)


class TestConsumer(object):
    def test_batch_size(self) -> None:
        topic = Topic("topic")
        payloads = [KafkaPayload(None, f"{i}".encode("utf-8")) for i in [1, 2, 3]]
        consumer: DummyConsumer[KafkaPayload] = DummyConsumer(
            {Partition(topic, 0): payloads}
        )

        worker = FakeWorker()
        batching_consumer = BatchingConsumer(
            consumer,
            topic,
            worker=worker,
            max_batch_size=2,
            max_batch_time=100,
            metrics=DummyMetricsBackend(strict=True),
        )

        for _ in range(len(payloads)):
            batching_consumer._run_once()

        batching_consumer._shutdown()

        assert worker.processed == [payload.value for payload in payloads]
        assert worker.flushed == [[payload.value for payload in payloads[:2]]]
        assert consumer.commit_offsets_calls == 1
        assert consumer.close_calls == 1

    @patch("time.time")
    def test_batch_time(self, mock_time: Any) -> None:
        topic = Topic("topic")
        consumer: DummyConsumer[KafkaPayload] = DummyConsumer({Partition(topic, 0): []})

        worker = FakeWorker()
        batching_consumer = BatchingConsumer(
            consumer,
            topic,
            worker=worker,
            max_batch_size=100,
            max_batch_time=2000,
            metrics=DummyMetricsBackend(strict=True),
        )

        mock_time.return_value = time.mktime(datetime(2018, 1, 1, 0, 0, 0).timetuple())

        consumer.extend(
            {
                Partition(topic, 0): [
                    KafkaPayload(None, f"{i}".encode("utf-8")) for i in [1, 2, 3]
                ],
            }
        )

        for _ in range(3):
            batching_consumer._run_once()

        mock_time.return_value = time.mktime(datetime(2018, 1, 1, 0, 0, 1).timetuple())

        consumer.extend(
            {
                Partition(topic, 0): [
                    KafkaPayload(None, f"{i}".encode("utf-8")) for i in [4, 5, 6]
                ],
            }
        )

        for x in range(3):
            batching_consumer._run_once()

        mock_time.return_value = time.mktime(datetime(2018, 1, 1, 0, 0, 5).timetuple())

        consumer.extend(
            {
                Partition(topic, 0): [
                    KafkaPayload(None, f"{i}".encode("utf-8")) for i in [7, 8, 9]
                ],
            }
        )

        for x in range(3):
            batching_consumer._run_once()

        batching_consumer._shutdown()

        assert worker.processed == [
            b"1",
            b"2",
            b"3",
            b"4",
            b"5",
            b"6",
            b"7",
            b"8",
            b"9",
        ]
        assert worker.flushed == [[b"1", b"2", b"3", b"4", b"5", b"6"]]
        assert consumer.commit_offsets_calls == 1
        assert consumer.close_calls == 1
