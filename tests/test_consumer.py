from datetime import datetime
from mock import patch
import time
from datadog import statsd

from base import BaseTest

from snuba.consumer import AbstractBatchWorker, BatchingKafkaConsumer


class FakeKafkaMessage(object):
    def __init__(self, value):
        self._value = value

    def error(self):
        return None

    def value(self):
        return self._value


class FakeKafkaConsumer(object):
    def __init__(self):
        self.items = []
        self.commit_calls = 0
        self.close_calls = 0

    def poll(self, timeout=None):
        return FakeKafkaMessage(self.items.pop(0))

    def commit(self, message, asynchronous):
        self.commit_calls += 1

    def close(self):
        self.close_calls += 1


class FakeBatchingKafkaConsumer(BatchingKafkaConsumer):
    def create_consumer(self, topic, bootstrap_server, group_id):
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


class TestConsumer(BaseTest):
    def test_batch_size(self):
        consumer = FakeBatchingKafkaConsumer(
            'topic',
            worker=FakeWorker(),
            max_batch_size=2,
            max_batch_time=100,
            metrics=statsd,
            bootstrap_server=None,
            group_id=None,
        )

        consumer.consumer.items = [1, 2, 3]
        for x in xrange(len(consumer.consumer.items)):
            consumer._run_once()
        consumer._shutdown()

        assert consumer.worker.processed == [1, 2, 3]
        assert consumer.worker.flushed == [[1, 2]]
        assert consumer.worker.shutdown_calls == 1
        assert consumer.consumer.commit_calls == 1
        assert consumer.consumer.close_calls == 1

    @patch('time.time')
    def test_batch_time(self, mock_time):
        consumer = FakeBatchingKafkaConsumer(
            'topic',
            worker=FakeWorker(),
            max_batch_size=100,
            max_batch_time=2000,
            metrics=statsd,
            bootstrap_server=None,
            group_id=None,
        )

        mock_time.return_value = time.mktime(datetime(2018, 1, 1, 0, 0, 0).timetuple())
        consumer.consumer.items = [1, 2, 3]
        for x in xrange(len(consumer.consumer.items)):
            consumer._run_once()

        mock_time.return_value = time.mktime(datetime(2018, 1, 1, 0, 0, 1).timetuple())
        consumer.consumer.items = [4, 5, 6]
        for x in xrange(len(consumer.consumer.items)):
            consumer._run_once()

        mock_time.return_value = time.mktime(datetime(2018, 1, 1, 0, 0, 5).timetuple())
        consumer.consumer.items = [7, 8, 9]
        for x in xrange(len(consumer.consumer.items)):
            consumer._run_once()

        consumer._shutdown()

        assert consumer.worker.processed == [1, 2, 3, 4, 5, 6, 7, 8, 9]
        assert consumer.worker.flushed == [[1, 2, 3, 4, 5, 6]]
        assert consumer.worker.shutdown_calls == 1
        assert consumer.consumer.commit_calls == 1
        assert consumer.consumer.close_calls == 1
