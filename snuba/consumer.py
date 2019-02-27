import logging
import simplejson as json
import six

from batching_kafka_consumer import AbstractBatchWorker

from .writer import write_rows
from .processor import Processor


logger = logging.getLogger('snuba.consumer')


class ConsumerWorker(AbstractBatchWorker):
    """
    ConsumerWorker processes the raw stream of events coming
    from Kafka, validates that they have the correct format
    and does one of 2 things:

        - If they are INSERTs, creates insertable rows from them
          and batches and inserts those rows to clickhouse.
        - If they are REPLACEs, forwards them to the replacements
          Kafka topic, so that the Replacements consumer can deal
          with them.
    """
    def __init__(self, clickhouse, dataset, producer, replacements_topic, metrics=None):
        self.clickhouse = clickhouse
        self.dataset = dataset
        self.producer = producer
        self.replacements_topic = replacements_topic
        self.metrics = metrics

    def process_message(self, message):
        value = json.loads(message.value())

        validated = self.dataset.PROCESSOR.validate_message(value)
        if validated is None:
            return None

        action_type, data = validated

        if action_type == Processor.INSERT:
            # Add these things to the message that we only know about once
            # we've consumed it.
            data.update({
                'offset': message.offset(),
                'partition': message.partition()
            })
            processed = self.dataset.PROCESSOR.process_insert(data)
            if processed is None:
                return None

        elif action_type == Processor.REPLACE:
            processed = data

        return (action_type, processed)

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
            if action_type == Processor.INSERT:
                inserts.append(data)
            elif action_type == Processor.REPLACE:
                replacements.append(data)

        if inserts:
            write_rows(self.clickhouse, self.dataset, inserts)

            if self.metrics:
                self.metrics.timing('inserts', len(inserts))

        if replacements:
            key_func = self.dataset.PROCESSOR.key_function
            for replacement in replacements:
                self.producer.produce(
                    self.replacements_topic,
                    key=key_func(replacement),
                    value=json.dumps(replacement).encode('utf-8'),
                    on_delivery=self.delivery_callback,
                )

            self.producer.flush()

    def shutdown(self):
        pass
