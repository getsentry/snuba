import abc
import logging
import time

from kafka import KafkaConsumer
from kafka import ConsumerRebalanceListener


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


class RebalanceListener(ConsumerRebalanceListener):
    """Handles flushing any locally buffered events (and Kafka offsets)
    when group rebalances happen.

    See `ConsumerRebalanceListener` documentation for more info.
    """

    def __init__(self, batching_consumer):
        self.batching_consumer = batching_consumer

    def on_partitions_revoked(self, revoked):
        "Flush the batch (if any) and commit Kafka offsets."
        logger.debug("Partitons revoked: %s" % revoked)

        self.batching_consumer._flush()

    def on_partitions_assigned(self, assigned):
        logger.debug("New partitions assigned: %s" % assigned)


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

    def __init__(self, topic, worker, max_batch_size, max_batch_time, **kwargs):
        assert isinstance(worker, AbstractBatchWorker)
        self.worker = worker

        self.max_batch_size = max_batch_size
        self.max_batch_time = max_batch_time

        self.shutdown = False
        self.batch = []
        self.timer = None

        self.consumer = self.create_consumer(topic, **kwargs)

    def create_consumer(self, topic, **kwargs):
        # Ensure the caller didn't expct auto commit to work
        assert not kwargs.get('enable_auto_commit', False)

        # `consumer_timeout_ms` configures the longest amount of time (in millis) we have
        # to wait for our main loop to run if no messages are received on a Kafka topic,
        # which means it needs to be the minimum of our `max_batch_time` and some reasonable
        # default that doesn't leave a user waiting forever for a shutdown to complete
        consumer_timeout_ms = kwargs.pop('consumer_timeout_ms', float('inf'))
        consumer_timeout_ms = min(self.max_batch_time, consumer_timeout_ms)
        consumer_timeout_ms = min(1000, consumer_timeout_ms)

        # The KafkaConsumer constructor doesn't let us setup a RebalanceListener
        # so we have to construct it, then call subscribe.
        consumer = KafkaConsumer(
            enable_auto_commit=False,
            consumer_timeout_ms=consumer_timeout_ms,
            **kwargs
        )

        consumer.subscribe(
            topics=(topic, ),
            listener=RebalanceListener(self)
        )

        return consumer

    def run(self):
        "The main run loop, see class docstring for more information."

        logger.debug("Starting")

        while not self.shutdown:
            self._run_once()

        self._shutdown()

    def signal_shutdown(self):
        """Tells the `BatchingKafkaConsumer` to shutdown on the next run loop iteration.
        Typically called from a signal handler."""
        logger.debug("Shutdown signalled")

        self.shutdown = True

    def _run_once(self):
        self._flush()

        # this iterator will run unless no message is received by `consumer_timeout_ms`
        # (which we set in the constructor), at which point we begin the infinite
        # loop again. this gives us a chance to check for shutdown (or flush based on time)
        # even if a topic is not seeing much (or any) traffic
        for msg in self.consumer:
            # start the timer only after the first message for this batch is seen
            if not self.timer:
                self.timer = self.max_batch_time / 1000.0 + time.time()

            result = self.worker.process_message(msg)
            self.batch.append(result)

            self._flush()
            if self.shutdown:
                break

    def _shutdown(self):
        logger.debug("Stopping")

        # force flush the batch because it is unlikely that we hit a
        # size or time threshold just as we were told to shutdown
        self._flush(force=True)

        # tell the consumer to shutdown, and close the consumer
        logger.debug("Stopping worker")
        self.worker.shutdown()
        logger.debug("Stopping consumer")
        self.consumer.close()

        logger.debug("Stopped")

    def _flush(self, force=False):
        """Decides whether the `BatchingKafkaConsumer` should flush because of either
        batch size or time. If so, delegate to the worker, clear the current batch,
        and commit offsets to Kafka."""
        if len(self.batch) > 0:
            batch_by_size = len(self.batch) >= self.max_batch_size
            batch_by_time = self.timer and time.time() > self.timer
            if (force or batch_by_size or batch_by_time):
                logger.debug(
                    "Flushing %s items: forced:%s size:%s time:%s" % (
                        len(self.batch), force, batch_by_size, batch_by_time)
                )

                logger.debug("Flushing batch via worker")
                t = time.time()
                self.worker.flush_batch(self.batch)
                logger.debug("Worker flush took %ss" % (time.time() - t))

                self.batch = []
                self.timer = None

                logger.debug("Committing Kafka offsets")
                t = time.time()
                self.consumer.commit()
                logger.debug("Kafka offset commit took %ss" % (time.time() - t))
