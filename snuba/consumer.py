import logging
import simplejson as json
import six

from batching_kafka_consumer import AbstractBatchWorker

from .writer import write_rows


logger = logging.getLogger('snuba.consumer')


class InvalidMessageType(Exception):
    pass


class InvalidMessageVersion(Exception):
    pass


class InvalidActionType(Exception):
    pass


# action types
INSERT = object()
REPLACE = object()

class ConsumerWorker(AbstractBatchWorker):
    """
    ConsumerWorker processes the raw stream of events coming
    from Kafka, validates that they have the correct format
    and does one of 2 things:

        - If they are INSERTs, creates rows to insert from them
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

        validated = self.validate_message(value)
        if validated is None:
            return None

        action_type, data = validated

        if action_type == INSERT:
            # Add these things to the message that we only know about once
            # we've consumed it.
            data.update({
                'offset': message.offset(),
                'partition': message.partition()
            })
            processed = self.dataset.PROCESSOR.process_insert(data)
            if processed is None:
                return None

        elif action_type == REPLACE:
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
            if action_type == INSERT:
                inserts.append(data)
            elif action_type == REPLACE:
                replacements.append(data)

        if inserts:
            write_rows(self.clickhouse, self.dataset, inserts)

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

    @staticmethod
    def validate_message(message):
        """
        Validate a raw mesage from kafka, with 1 of 3 results.
          - A tuple of (action_type, data) is returned to be handled by the proper
            handler/processor for that action type.
          - None is returned, indicating we should silently ignore this message.
          - An exeption is raised, inditcating something is wrong and we should stop.
        """
        action_type = None
        data = None

        if isinstance(message, dict):
            # deprecated unwrapped event message == insert
            action_type = INSERT
            data = message
        elif isinstance(message, (list, tuple)) and len(message) >= 2:
            version = message[0]

            if version in (0, 1, 2):
                # version 0: (0, 'insert', data)
                # version 1: (1, type, data, [state])
                #   NOTE: types 'delete_groups', 'merge' and 'unmerge' are ignored
                # version 2: (2, type, data, [state])
                type_, event = message[1:3]
                if type_ == 'insert':
                    action_type = INSERT
                    data = event
                else:
                    if version == 0:
                        raise InvalidMessageType("Invalid message type: {}".format(type_))
                    elif version == 1:
                        if type_ in ('delete_groups', 'merge', 'unmerge'):
                            # these didn't contain the necessary data to handle replacements
                            return None
                        else:
                            raise InvalidMessageType("Invalid message type: {}".format(type_))
                    elif version == 2:
                        # we temporarily sent these invalid message types from Sentry
                        if type_ in ('delete_groups', 'merge'):
                            return None

                        if type_ in ('start_delete_groups', 'start_merge', 'start_unmerge',
                                     'start_delete_tag', 'end_delete_groups', 'end_merge',
                                     'end_unmerge', 'end_delete_tag'):
                            action_type = REPLACE
                            data = (six.text_type(event['project_id']), message)
                        else:
                            raise InvalidMessageType("Invalid message type: {}".format(type_))

        if action_type is None:
            raise InvalidMessageVersion("Unknown message format: " + str(message))

        if action_type not in (INSERT, REPLACE):
            raise InvalidActionType("Invalid action type: {}".format(action_type))

        if data is None:
            return None

        return (action_type, data)

    def shutdown(self):
        pass
