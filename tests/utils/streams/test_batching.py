import time
from datetime import datetime
from typing import Any, MutableSequence, Sequence, Optional
from unittest.mock import patch

from confluent_kafka import Message

from snuba.utils.metrics.backends.dummy import DummyMetricsBackend
from snuba.utils.streams.batching import AbstractBatchWorker, BatchingKafkaConsumer
from tests.backends.confluent_kafka import FakeConfluentKafkaConsumer, FakeConfluentKafkaMessage, FakeConfluentKafkaProducer


class FakeWorker(AbstractBatchWorker):
    def __init__(self) -> None:
        self.processed: MutableSequence[Optional[Any]] = []
        self.flushed: MutableSequence[Sequence[Any]] = []

    def process_message(self, message: Message) -> Optional[Any]:
        self.processed.append(message.value())
        return message.value()

    def flush_batch(self, batch: Sequence[Any]) -> None:
        self.flushed.append(batch)


class TestConsumer(object):
    def test_batch_size(self) -> None:
        consumer = FakeConfluentKafkaConsumer()
        worker = FakeWorker()
        producer = FakeConfluentKafkaProducer()
        batching_consumer = BatchingKafkaConsumer(
            consumer,
            'topic',
            worker=worker,
            max_batch_size=2,
            max_batch_time=100,
            group_id='group',
            commit_log_topic='commits',
            producer=producer,
            metrics=DummyMetricsBackend(strict=True),
        )

        consumer.items = [FakeConfluentKafkaMessage('topic', 0, i, i) for i in [1, 2, 3]]
        for x in range(len(consumer.items)):
            batching_consumer._run_once()
        batching_consumer._shutdown()

        assert worker.processed == [1, 2, 3]
        assert worker.flushed == [[1, 2]]
        assert consumer.commit_calls == 1
        assert consumer.close_calls == 1

        assert len(producer.messages) == 1
        commit_message = producer.messages[0]
        assert commit_message.topic() == 'commits'
        assert commit_message.key() == '{}:{}:{}'.format('topic', 0, 'group').encode('utf-8')
        assert commit_message.value() == '{}'.format(2 + 1).encode('utf-8')  # offsets are last processed message offset + 1

    @patch('time.time')
    def test_batch_time(self, mock_time: Any) -> None:
        consumer = FakeConfluentKafkaConsumer()
        worker = FakeWorker()
        producer = FakeConfluentKafkaProducer()
        batching_consumer = BatchingKafkaConsumer(
            consumer,
            'topic',
            worker=worker,
            max_batch_size=100,
            max_batch_time=2000,
            group_id='group',
            commit_log_topic='commits',
            producer=producer,
            metrics=DummyMetricsBackend(strict=True),
        )

        mock_time.return_value = time.mktime(datetime(2018, 1, 1, 0, 0, 0).timetuple())
        consumer.items = [FakeConfluentKafkaMessage('topic', 0, i, i) for i in [1, 2, 3]]
        for x in range(len(consumer.items)):
            batching_consumer._run_once()

        mock_time.return_value = time.mktime(datetime(2018, 1, 1, 0, 0, 1).timetuple())
        consumer.items = [FakeConfluentKafkaMessage('topic', 0, i, i) for i in [4, 5, 6]]
        for x in range(len(consumer.items)):
            batching_consumer._run_once()

        mock_time.return_value = time.mktime(datetime(2018, 1, 1, 0, 0, 5).timetuple())
        consumer.items = [FakeConfluentKafkaMessage('topic', 0, i, i) for i in [7, 8, 9]]
        for x in range(len(consumer.items)):
            batching_consumer._run_once()

        batching_consumer._shutdown()

        assert worker.processed == [1, 2, 3, 4, 5, 6, 7, 8, 9]
        assert worker.flushed == [[1, 2, 3, 4, 5, 6]]
        assert consumer.commit_calls == 1
        assert consumer.close_calls == 1

        assert len(producer.messages) == 1
        commit_message = producer.messages[0]
        assert commit_message.topic() == 'commits'
        assert commit_message.key() == '{}:{}:{}'.format('topic', 0, 'group').encode('utf-8')
        assert commit_message.value() == '{}'.format(6 + 1).encode('utf-8')  # offsets are last processed message offset + 1
