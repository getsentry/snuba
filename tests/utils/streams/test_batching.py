import time
from datetime import datetime
from typing import Any, Callable, Mapping, MutableMapping, MutableSequence, Sequence, Optional
from unittest.mock import patch

from snuba.utils.metrics.backends.dummy import DummyMetricsBackend
from snuba.utils.streams.consumers.consumer import Consumer
from snuba.utils.streams.consumers.backends.abstract import ConsumerBackend
from snuba.utils.streams.consumers.backends.kafka import KafkaMessage, TopicPartition
from snuba.utils.streams.batching import AbstractBatchWorker, BatchingConsumer


class FakeKafkaConsumerBackend(ConsumerBackend[TopicPartition, int, bytes]):
    def __init__(self):
        self.items: MutableSequence[KafkaMessage] = []
        self.commit_calls = 0
        self.close_calls = 0
        self.positions: MutableMapping[TopicPartition, int] = {}

    def subscribe(
        self,
        topics: Sequence[str],
        on_assign: Optional[Callable[[Sequence[TopicPartition]], None]] = None,
        on_revoke: Optional[Callable[[Sequence[TopicPartition]], None]] = None,
    ) -> None:
        pass  # XXX: This is a bit of a smell.

    def unsubscribe(self) -> None:
        pass  # XXX: This is a bit of a smell.

    def poll(
        self, timeout: Optional[float] = None
    ) -> Optional[KafkaMessage]:
        try:
            message = self.items.pop(0)
        except IndexError:
            return None

        self.positions[message.stream] = message.get_next_offset()

        return message

    def tell(self) -> Mapping[TopicPartition, int]:
        return self.__positions

    def seek(self, offsets: Mapping[TopicPartition, int]) -> None:
        raise NotImplementedError  # XXX: This is a bit more of a smell.

    def pause(self, streams: Sequence[TopicPartition]):
        raise NotImplementedError

    def resume(self, streams: Sequence[TopicPartition]):
        raise NotImplementedError

    def commit(self) -> Mapping[TopicPartition, int]:
        self.commit_calls += 1
        return self.positions

    def close(self) -> None:
        self.close_calls += 1


class FakeWorker(AbstractBatchWorker[KafkaMessage, Any]):
    def __init__(self) -> None:
        self.processed: MutableSequence[Optional[Any]] = []
        self.flushed: MutableSequence[Sequence[Any]] = []

    def process_message(self, message: KafkaMessage) -> Optional[Any]:
        self.processed.append(message.value)
        return message.value

    def flush_batch(self, batch: Sequence[Any]) -> None:
        self.flushed.append(batch)


class TestConsumer(object):
    def test_batch_size(self) -> None:
        backend = FakeKafkaConsumerBackend()
        worker = FakeWorker()
        batching_consumer = BatchingConsumer(
            Consumer(backend),
            'topic',
            worker=worker,
            max_batch_size=2,
            max_batch_time=100,
            metrics=DummyMetricsBackend(strict=True),
        )

        backend.items = [KafkaMessage(TopicPartition('topic', 0), i, f'{i}'.encode('utf-8')) for i in [1, 2, 3]]
        for x in range(len(backend.items)):
            batching_consumer._run_once()
        batching_consumer._shutdown()

        assert worker.processed == [b'1', b'2', b'3']
        assert worker.flushed == [[b'1', b'2']]
        assert backend.commit_calls == 1
        assert backend.close_calls == 1

    @patch('time.time')
    def test_batch_time(self, mock_time: Any) -> None:
        backend = FakeKafkaConsumerBackend()
        worker = FakeWorker()
        batching_consumer = BatchingConsumer(
            Consumer(backend),
            'topic',
            worker=worker,
            max_batch_size=100,
            max_batch_time=2000,
            metrics=DummyMetricsBackend(strict=True),
        )

        mock_time.return_value = time.mktime(datetime(2018, 1, 1, 0, 0, 0).timetuple())
        backend.items = [KafkaMessage(TopicPartition('topic', 0), i, f'{i}'.encode('utf-8')) for i in [1, 2, 3]]
        for x in range(len(backend.items)):
            batching_consumer._run_once()

        mock_time.return_value = time.mktime(datetime(2018, 1, 1, 0, 0, 1).timetuple())
        backend.items = [KafkaMessage(TopicPartition('topic', 0), i, f'{i}'.encode('utf-8')) for i in [4, 5, 6]]
        for x in range(len(backend.items)):
            batching_consumer._run_once()

        mock_time.return_value = time.mktime(datetime(2018, 1, 1, 0, 0, 5).timetuple())
        backend.items = [KafkaMessage(TopicPartition('topic', 0), i, f'{i}'.encode('utf-8')) for i in [7, 8, 9]]
        for x in range(len(backend.items)):
            batching_consumer._run_once()

        batching_consumer._shutdown()

        assert worker.processed == [b'1', b'2', b'3', b'4', b'5', b'6', b'7', b'8', b'9']
        assert worker.flushed == [[b'1', b'2', b'3', b'4', b'5', b'6']]
        assert backend.commit_calls == 1
        assert backend.close_calls == 1
