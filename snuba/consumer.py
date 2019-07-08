import collections
import logging
import simplejson as json
import six

from batching_kafka_consumer import AbstractBatchWorker

from .writer import write_rows


logger = logging.getLogger('snuba.consumer')

KafkaMessageMetadata = collections.namedtuple(
    'KafkaMessageMetadata',
    'offset partition'
)


class InvalidActionType(Exception):
    pass


class ConsumerWorker(AbstractBatchWorker):
    def __init__(self, clickhouse, dataset, producer, replacements_topic, metrics=None):
        self.clickhouse = clickhouse
        self.__dataset = dataset
        self.producer = producer
        self.replacements_topic = replacements_topic
        self.metrics = metrics

    def process_message(self, message):
        # TODO: consider moving this inside the processor so we can do a quick
        # processing of messages we want to filter out without fully parsing the
        # json.
        processor = self.__dataset.get_processor()
        value = json.loads(message.value())
        metadata = KafkaMessageMetadata(offset=message.offset(), partition=message.partition())
        processed = processor.process_message(value, metadata)
        if processed is None:
            return None

        action_type, processed_message = processed

        if action_type == processor.INSERT:
            result = self.__dataset.row_from_processed_message(processed_message)
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
        processor = self.__dataset.get_processor()
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
                self.__dataset,
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
