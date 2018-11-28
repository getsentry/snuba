import abc
import logging
import simplejson as json
import six
import time

from confluent_kafka import Consumer, KafkaError, KafkaException

from . import processor, settings
from .clickhouse import ALL_COLUMNS
from .writer import row_from_processed_event, write_rows


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

        If this method returns `None` it is not added to the batch.

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

    # Set of logical (not literal) offsets to not publish to the commit log.
    # https://github.com/confluentinc/confluent-kafka-python/blob/443177e1c83d9b66ce30f5eb8775e062453a738b/tests/test_enums.py#L22-L25
    LOGICAL_OFFSETS = frozenset([OFFSET_BEGINNING, OFFSET_END, OFFSET_STORED, OFFSET_INVALID])

    def __init__(self, topics, worker, max_batch_size, max_batch_time, metrics,
                 bootstrap_servers, group_id, producer=None, commit_log_topic=None,
                 auto_offset_reset='error',
                 queued_max_messages_kbytes=settings.DEFAULT_QUEUED_MAX_MESSAGE_KBYTES,
                 queued_min_messages=settings.DEFAULT_QUEUED_MIN_MESSAGES):
        assert isinstance(worker, AbstractBatchWorker)
        self.worker = worker

        self.max_batch_size = max_batch_size
        self.max_batch_time = max_batch_time
        self.metrics = metrics

        self.group_id = group_id
        self.commit_log_topic = commit_log_topic

        self.shutdown = False
        self.batch = []
        self.timer = None

        if not isinstance(topics, (list, tuple)):
            topics = [topics]
        elif isinstance(topics, tuple):
            topics = list(topics)

        self.consumer = self.create_consumer(
            topics, bootstrap_servers, group_id, auto_offset_reset,
            queued_max_messages_kbytes, queued_min_messages
        )

        self.producer = producer

    def create_consumer(self, topics, bootstrap_servers, group_id, auto_offset_reset,
            queued_max_messages_kbytes, queued_min_messages):

        consumer_config = {
            'enable.auto.commit': False,
            'bootstrap.servers': ','.join(bootstrap_servers),
            'group.id': group_id,
            'default.topic.config': {
                'auto.offset.reset': auto_offset_reset,
            },
            # overridden to reduce memory usage when there's a large backlog
            'queued.max.messages.kbytes': queued_max_messages_kbytes,
            'queued.min.messages': queued_min_messages,
        }

        consumer = Consumer(consumer_config)

        def on_partitions_assigned(consumer, partitons):
            logger.info("New partitions assigned: %s" % partitons)

        def on_partitions_revoked(consumer, partitons):
            "Reset the current in-memory batch, letting the next consumer take over where we left off."
            logger.info("Partitions revoked: %s" % partitons)
            self._flush(force=True)

        consumer.subscribe(
            topics,
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

        if self.commit_log_topic:
            self.producer.poll(0.0)

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
        if result is not None:
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
                self._commit()
                duration = int((time.time() - t) * 1000)
                logger.debug("Kafka offset commit took %sms" % duration)

                self._reset_batch()

    def _commit_message_delivery_callback(self, error, message):
        if error is not None:
            raise Exception(error.str())

    def _commit(self):
        retries = 3
        while True:
            try:
                offsets = self.consumer.commit(asynchronous=False)
                logger.debug("Committed offsets: %s" % offsets)
                break  # success
            except KafkaException as e:
                if e.args[0].code() in (KafkaError.REQUEST_TIMED_OUT,
                                        KafkaError.NOT_COORDINATOR_FOR_GROUP,
                                        KafkaError._WAIT_COORD):
                    logger.warning("Commit failed: %s (%d retries)" % (str(e), retries))
                    if retries <= 0:
                        raise
                    retries -= 1
                    time.sleep(1)
                    continue
                else:
                    raise

        if self.commit_log_topic:
            for item in offsets:
                if item.offset in self.LOGICAL_OFFSETS:
                    logger.debug('Skipped publishing logical offset (%r) to commit log for %s/%s', item.offset, item.topic, item.partiton)
                    continue
                elif item.offset < 0:
                    logger.warning('Found unexpected negative offset (%r) after commit for %s/%s', item.offset, item.topic, item.partiton)

                self.producer.produce(
                    self.commit_log_topic,
                    key='{}:{}:{}'.format(item.topic, item.partition, self.group_id).encode('utf-8'),
                    value='{}'.format(item.offset).encode('utf-8'),
                    on_delivery=self._commit_message_delivery_callback,
                )


class InvalidActionType(Exception):
    pass


class ConsumerWorker(AbstractBatchWorker):
    def __init__(self, clickhouse, dist_table_name, producer, replacements_topic, metrics=None):
        self.clickhouse = clickhouse
        self.dist_table_name = dist_table_name
        self.producer = producer
        self.replacements_topic = replacements_topic
        self.metrics = metrics

    def process_message(self, message):
        value = json.loads(message.value())

        processed = processor.process_message(value)
        if processed is None:
            return None

        action_type, processed_message = processed

        if action_type == processor.INSERT:
            processed_message['offset'] = message.offset()
            processed_message['partition'] = message.partition()

            result = row_from_processed_event(processed_message)
        elif action_type == processor.REPLACE:
            result = processed_message
        else:
            raise InvalidActionType("Invalid action type: {}".format(action_type))

        return (action_type, result)

    def delivery_callback(self, error, message):
        if error is not None:
            # errors are KafkaError objects and inherit from BaseException
            raise error

    def flush_batch(self, batch):
        """First write out all new INSERTs as a single batch, then reproduce any
        event replacements such as deletions, merges and unmerges."""
        inserts = []
        replacements = []

        for action_type, data in batch:
            if action_type == processor.INSERT:
                inserts.append(data)
            elif action_type == processor.REPLACE:
                replacements.append(data)

        if inserts:
            write_rows(
                self.clickhouse,
                self.dist_table_name,
                inserts
            )

            if self.metrics:
                self.metrics.timing('inserts', len(inserts))

        if replacements:
            for key, replacement in replacements:
                self.producer.produce(
                    self.replacements_topic,
                    key=six.text_type(key).encode('utf-8'),
                    value=json.dumps(replacement).encode('utf-8'),
                    on_delivery=self.delivery_callback,
                )

            self.producer.flush()

    def shutdown(self):
        pass
