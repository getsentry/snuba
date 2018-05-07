import abc
import logging
import time

from confluent_kafka import Consumer, KafkaError


logger = logging.getLogger('snuba.consumer')


class AbstractBatchWorker(object):
    """The `BatchingKafkaConsumer` requires an instance of this class to
    handle user provided work such as processing raw messages and flushing
    processed batches to a custom backend."""

    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def process_message(self, message):
        """Called with each (raw) Kafka message, allowing the worker to do
        incremental (preferablly local!) work on events. The object returned
        is put into the batch maintained by the `BatchingKafkaConsumer`.

        A simple example would be decoding the JSON value and extracting a few
        fields.
        """
        pass

    @abc.abstractmethod
    def flush_batch(self, batch):
        """Called with a list of pre-processed (by `process_message`) objects.
        The worker should write the batch of processed messages into whatever
        store(s) it is maintaining. Afterwards the Kafka offsets are committed.

        A simple example would be writing the batch to another Kafka topic.
        """
        pass

    @abc.abstractmethod
    def shutdown(self):
        """Called when the `BatchingKafkaConsumer` is shutting down (because it
        was signalled to do so). Provides the worker a chance to do any final
        cleanup.

        A simple example would be closing any remaining backend connections."""
        pass


class BatchingKafkaConsumer(object):
    """The `BatchingKafkaConsumer` is an abstraction over most Kafka consumer's main event
    loops. For this reason it uses inversion of control: the user provides an implementation
    for the `AbstractBatchWorker` and then the `BatchingKafkaConsumer` handles the rest.

    Main differences from the default KafkaConsumer are as follows:
    * Messages are processed locally (e.g. not written to an external datastore!) as they are
      read from Kafka, then added to an in-memory batch
    * Batches are flushed based on the batch size or time sent since the first message
      in the batch was recieved (e.g. "500 items or 1000ms")
    * Kafka offsets are not automatically committed! If they were, offsets might be committed
      for messages that are still sitting in an in-memory batch, or they might *not* be committed
      when messages are sent to an external datastore right before the consumer process dies
    * Instead, when a batch of items is flushed they are written to the external datastore and
      then Kafka offsets are immediately committed (in the same thread/loop)
    * Users need only provide an implementation of what it means to process a raw message
      and flush a batch of events

    NOTE: This does not eliminate the possibility of duplicates if the consumer process
    crashes between writing to its backend and commiting Kafka offsets. This should eliminate
    the possibility of *losing* data though. An "exactly once" consumer would need to store
    offsets in the external datastore and reconcile them on any partition rebalance.
    """

    def __init__(self, topic, worker, max_batch_size, max_batch_time, metrics, bootstrap_server, group_id):
        assert isinstance(worker, AbstractBatchWorker)
        self.worker = worker

        self.max_batch_size = max_batch_size
        self.max_batch_time = max_batch_time
        self.metrics = metrics

        self.shutdown = False
        self.batch = []
        self.timer = None

        self.consumer = self.create_consumer(topic, bootstrap_server, group_id)

    def create_consumer(self, topic, bootstrap_server, group_id):
        consumer_config = {
            'enable.auto.commit': False,
            'bootstrap.servers': bootstrap_server,
            'group.id': group_id,
            'default.topic.config': {
                'auto.offset.reset': 'error',
            }
        }

        consumer = Consumer(consumer_config)

        def on_partitions_assigned(consumer, partitons):
            logger.info("New partitions assigned: %s" % partitons)

        def on_partitions_revoked(consumer, partitons):
            "Reset the current in-memory batch, letting the next consumer take over where we left off."
            logger.info("Partitions revoked: %s" % partitons)
            self._flush(force=True)

        consumer.subscribe(
            [topic],
            on_assign=on_partitions_assigned,
            on_revoke=on_partitions_revoked,
        )

        return consumer

    def run(self):
        "The main run loop, see class docstring for more information."

        logger.debug("Starting")
        while not self.shutdown:
            self._run_once()

        self._shutdown()

    def _run_once(self):
        self._flush()

        msg = self.consumer.poll(timeout=1.0)

        if msg is None:
            return
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                return
            else:
                logger.error(msg.error())
                return

        self._handle_message(msg)

    def signal_shutdown(self):
        """Tells the `BatchingKafkaConsumer` to shutdown on the next run loop iteration.
        Typically called from a signal handler."""
        logger.debug("Shutdown signalled")

        self.shutdown = True

    def _handle_message(self, msg):
        # start the timer only after the first message for this batch is seen
        if not self.timer:
            self.timer = self.max_batch_time / 1000.0 + time.time()

        result = self.worker.process_message(msg)
        self.batch.append(result)

    def _shutdown(self):
        logger.debug("Stopping")

        # drop in-memory events, letting the next consumer take over where we left off
        self._reset_batch()

        # tell the consumer to shutdown, and close the consumer
        logger.debug("Stopping worker")
        self.worker.shutdown()
        logger.debug("Stopping consumer")
        self.consumer.close()

    logger.debug("Stopped")

    def _reset_batch(self):
        logger.debug("Resetting in-memory batch")
        self.batch = []
        self.timer = None

    def _flush(self, force=False):
        """Decides whether the `BatchingKafkaConsumer` should flush because of either
        batch size or time. If so, delegate to the worker, clear the current batch,
        and commit offsets to Kafka."""
        if len(self.batch) > 0:
            batch_by_size = len(self.batch) >= self.max_batch_size
            batch_by_time = self.timer and time.time() > self.timer
            if (force or batch_by_size or batch_by_time):
                logger.info(
                    "Flushing %s items: forced:%s size:%s time:%s" % (
                        len(self.batch), force, batch_by_size, batch_by_time)
                )

                logger.debug("Flushing batch via worker")
                t = time.time()
                self.worker.flush_batch(self.batch)
                duration = int((time.time() - t) * 1000)
                logger.info("Worker flush took %sms" % duration)
                if self.metrics:
                    self.metrics.timing('batch.flush', duration)

                logger.debug("Committing Kafka offsets")
                t = time.time()
                self.consumer.commit(asynchronous=False)
                duration = int((time.time() - t) * 1000)
                logger.debug("Kafka offset commit took %sms" % duration)

                self._reset_batch()
