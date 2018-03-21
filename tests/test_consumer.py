from base import BaseTest

from snuba.consumer import AbstractBatchWorker, BatchingKafkaConsumer


class FakeKafkaConsumer(object):
    def __init__(self):
        self.items = []
        self.commit_calls = 0
        self.close_calls = 0

    def __iter__(self):
        return iter(self.items)

    def commit(self):
        self.commit_calls += 1

    def close(self):
        self.close_calls += 1


class FakeBatchingKafkaConsumer(BatchingKafkaConsumer):
    def create_consumer(self, topic, **kwargs):
        return FakeKafkaConsumer()


class FakeWorker(AbstractBatchWorker):
    def __init__(self, *args, **kwargs):
        super(FakeWorker, self).__init__(*args, **kwargs)
        self.processed = []
        self.flushed = []
        self.shutdown_calls = 0

    def process_message(self, message):
        self.processed.append(message)
        return message

    def flush_batch(self, batch):
        self.flushed.append(batch)

    def shutdown(self):
        self.shutdown_calls += 1


class TestConsumer(BaseTest):
    def setup_method(self, test_method):
        super(TestConsumer, self).setup_method(test_method)

    def test(self):
        consumer = FakeBatchingKafkaConsumer(
            'topic',
            worker=FakeWorker(),
            max_batch_size=2,
            max_batch_time=100,
        )

        consumer.consumer.items = [1, 2, 3]

        consumer._run_once()
        consumer._shutdown()

        assert consumer.worker.processed == [1, 2, 3]
        assert consumer.worker.flushed == [[1, 2], [3]]
        assert consumer.worker.shutdown_calls == 1
        assert consumer.consumer.commit_calls == 2
        assert consumer.consumer.close_calls == 1
