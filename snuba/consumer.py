import abc
import time

from kafka import KafkaConsumer
from kafka import ConsumerRebalanceListener


class AbstractConsumerWorker(object):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def process_message(self, message):
        pass

    @abc.abstractmethod
    def flush_batch(self, batch):
        pass

    @abc.abstractmethod
    def shutdown(self):
        pass


class BatchingKafkaConsumer(object):
    def __init__(self, topic, consumer_worker, max_batch_size, max_batch_time, **configs):
        self.consumer_worker = consumer_worker
        self.max_batch_size = max_batch_size
        self.max_batch_time = max_batch_time
        self.shutdown = False
        self.batch = []
        self.timer = None

        class RebalanceListener(ConsumerRebalanceListener):
            def __init__(self, batching_consumer):
                self.batching_consumer = batching_consumer

            def on_partitions_revoked(self, revoked):
                self.batching_consumer._flush()

            def on_partitions_assigned(self, assigned):
                pass

        assert 'enable_auto_commit' not in configs or not configs['enable_auto_commit']
        consumer_timeout_ms = min(max_batch_time, configs.pop('consumer_timeout_ms', float('inf')))

        self.consumer = KafkaConsumer(
            enable_auto_commit=False,
            consumer_timeout_ms=consumer_timeout_ms,
            **configs
        )
        self.consumer.subscribe(topics=(topic, ), listener=RebalanceListener(self))

    def run(self):
        while True:
            for msg in self.consumer:
                if not self.timer:
                    self.timer = self.max_batch_time / 1000.0 + time.time()

                result = self.consumer_worker.process_message(msg)
                self.batch.append(result)

                self._flush()
                if self.shutdown:
                    break

            self._flush()
            if self.shutdown:
                break

        self._flush(force=True)
        self.consumer.close()

    def signal_shutdown(self):
        self.shutdown = True

    def _flush(self, force=False):
        if len(self.batch) > 0:
            batch_by_size = len(self.batch) >= self.max_batch_size
            batch_by_time = self.timer and time.time() > self.timer
            if (force or batch_by_size or batch_by_time):
                self.consumer_worker.flush_batch(self.batch)
                self.batch = []
                self.timer = None
                self.consumer.commit()
