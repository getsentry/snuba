import time
from datetime import datetime
from typing import (
    Any,
    Callable,
    Mapping,
    MutableMapping,
    MutableSequence,
    Sequence,
    Optional,
)
from unittest.mock import patch

from snuba.utils.metrics.backends.dummy import DummyMetricsBackend
from snuba.utils.streams.batching import AbstractBatchWorker, BatchingConsumer
from snuba.utils.streams.consumer import Consumer, KafkaPayload
from snuba.utils.streams.types import Message, Partition, Topic


class FakeConsumer(Consumer[KafkaPayload]):
    def __init__(self) -> None:
        self.items: MutableSequence[Message[KafkaPayload]] = []
        self.commit_calls = 0
        self.close_calls = 0
        self.positions: MutableMapping[Partition, int] = {}

    def subscribe(
        self,
        topics: Sequence[Topic],
        on_assign: Optional[Callable[[Mapping[Partition, int]], None]] = None,
        on_revoke: Optional[Callable[[Sequence[Partition]], None]] = None,
    ) -> None:
        pass  # XXX: This is a bit of a smell.

    def poll(self, timeout: Optional[float] = None) -> Optional[Message[KafkaPayload]]:
        try:
            message = self.items.pop(0)
        except IndexError:
            return None

        return message

    def stage_offsets(self, positions: Mapping[Partition, int]) -> None:
        self.positions.update(positions)

    def commit_offsets(self) -> Mapping[Partition, int]:
        self.commit_calls += 1
        positions = self.positions.copy()
        self.positions.clear()
        return positions

    def close(self, timeout: Optional[float] = None) -> None:
        self.close_calls += 1


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
        consumer = FakeConsumer()
        worker = FakeWorker()
        batching_consumer = BatchingConsumer(
            consumer,
            Topic("topic"),
            worker=worker,
            max_batch_size=2,
            max_batch_time=100,
            metrics=DummyMetricsBackend(strict=True),
        )

        consumer.items = [
            Message(
                Partition(Topic("topic"), 0),
                i,
                KafkaPayload(None, f"{i}".encode("utf-8")),
                datetime.now(),
            )
            for i in [1, 2, 3]
        ]
        for x in range(len(consumer.items)):
            batching_consumer._run_once()
        batching_consumer._shutdown()

        assert worker.processed == [b"1", b"2", b"3"]
        assert worker.flushed == [[b"1", b"2"]]
        assert consumer.commit_calls == 1
        assert consumer.close_calls == 1

    @patch("time.time")
    def test_batch_time(self, mock_time: Any) -> None:
        consumer = FakeConsumer()
        worker = FakeWorker()
        batching_consumer = BatchingConsumer(
            consumer,
            Topic("topic"),
            worker=worker,
            max_batch_size=100,
            max_batch_time=2000,
            metrics=DummyMetricsBackend(strict=True),
        )

        mock_time.return_value = time.mktime(datetime(2018, 1, 1, 0, 0, 0).timetuple())
        consumer.items = [
            Message(
                Partition(Topic("topic"), 0),
                i,
                KafkaPayload(None, f"{i}".encode("utf-8")),
                datetime.now(),
            )
            for i in [1, 2, 3]
        ]
        for x in range(len(consumer.items)):
            batching_consumer._run_once()

        mock_time.return_value = time.mktime(datetime(2018, 1, 1, 0, 0, 1).timetuple())
        consumer.items = [
            Message(
                Partition(Topic("topic"), 0),
                i,
                KafkaPayload(None, f"{i}".encode("utf-8")),
                datetime.now(),
            )
            for i in [4, 5, 6]
        ]
        for x in range(len(consumer.items)):
            batching_consumer._run_once()

        mock_time.return_value = time.mktime(datetime(2018, 1, 1, 0, 0, 5).timetuple())
        consumer.items = [
            Message(
                Partition(Topic("topic"), 0),
                i,
                KafkaPayload(None, f"{i}".encode("utf-8")),
                datetime.now(),
            )
            for i in [7, 8, 9]
        ]
        for x in range(len(consumer.items)):
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
        assert consumer.commit_calls == 1
        assert consumer.close_calls == 1
