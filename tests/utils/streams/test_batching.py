import time
from datetime import datetime
from typing import (
    Any,
    MutableSequence,
    Sequence,
)
from unittest.mock import patch

from snuba.utils.metrics.backends.dummy import DummyMetricsBackend
from snuba.utils.streams.batching import AbstractBatchWorker, BatchingConsumer
from snuba.utils.streams.dummy import DummyConsumer
from snuba.utils.streams.types import Message, Partition, Topic


class FakeWorker(AbstractBatchWorker[int, int]):
    def __init__(self) -> None:
        self.processed: MutableSequence[int] = []
        self.flushed: MutableSequence[Sequence[int]] = []

    def process_message(self, message: Message[int]) -> int:
        self.processed.append(message.payload)
        return message.payload

    def flush_batch(self, batch: Sequence[int]) -> None:
        self.flushed.append(batch)


class TestConsumer(object):
    def test_batch_size(self) -> None:
        topic = Topic("topic")
        consumer: DummyConsumer[int] = DummyConsumer({Partition(topic, 0): [1, 2, 3]})

        worker = FakeWorker()
        batching_consumer = BatchingConsumer(
            consumer,
            topic,
            worker=worker,
            max_batch_size=2,
            max_batch_time=100,
            metrics=DummyMetricsBackend(strict=True),
        )

        for _ in range(3):
            batching_consumer._run_once()

        batching_consumer._shutdown()

        assert worker.processed == [1, 2, 3]
        assert worker.flushed == [[1, 2]]
        assert consumer.commit_offsets_calls == 1
        assert consumer.close_calls == 1

    @patch("time.time")
    def test_batch_time(self, mock_time: Any) -> None:
        topic = Topic("topic")
        consumer: DummyConsumer[int] = DummyConsumer({Partition(topic, 0): []})

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

        consumer.extend({Partition(topic, 0): [1, 2, 3]})

        for _ in range(3):
            batching_consumer._run_once()

        mock_time.return_value = time.mktime(datetime(2018, 1, 1, 0, 0, 1).timetuple())

        consumer.extend({Partition(topic, 0): [4, 5, 6]})

        for _ in range(3):
            batching_consumer._run_once()

        mock_time.return_value = time.mktime(datetime(2018, 1, 1, 0, 0, 5).timetuple())

        consumer.extend({Partition(topic, 0): [7, 8, 9]})

        for _ in range(3):
            batching_consumer._run_once()

        batching_consumer._shutdown()

        assert worker.processed == [1, 2, 3, 4, 5, 6, 7, 8, 9]
        assert worker.flushed == [[1, 2, 3, 4, 5, 6]]
        assert consumer.commit_offsets_calls == 1
        assert consumer.close_calls == 1
