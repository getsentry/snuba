import logging
import time
from itertools import chain

from snuba.util import settings_override


logger = logging.getLogger('snuba.perf')


class FakeKafkaMessage(object):
    def __init__(self, topic, partition, offset, value, key=None, error=None):
        self._topic = topic
        self._partition = partition
        self._offset = offset
        self._value = value
        self._key = key
        self._error = error

    def topic(self):
        return self._topic

    def partition(self):
        return self._partition

    def offset(self):
        return self._offset

    def value(self):
        return self._value

    def key(self):
        return self._key

    def error(self):
        return self._error


def get_messages(events_file):
    "Create a FakeKafkaMessage for each JSON event in the file."
    messages = []
    raw_events = open(events_file).readlines()
    for raw_event in raw_events:
        messages.append(FakeKafkaMessage('events', 1, 0, raw_event))
    return messages


def run(events_file, clickhouse, table_name, repeat=1):
    from snuba.clickhouse import get_table_definition, get_test_engine
    from snuba.consumer import ConsumerWorker

    clickhouse.execute(
        get_table_definition(
            name=table_name,
            engine=get_test_engine(),
        )
    )

    consumer = ConsumerWorker(
        clickhouse=clickhouse,
        dist_table_name=table_name,
        producer=None,
        replacements_topic=None,
    )

    messages = get_messages(events_file)
    messages = chain(*([messages] * repeat))

    time_start = time.time()
    processed = []

    with settings_override({'DISCARD_OLD_EVENTS': False}):
        for message in messages:
            result = consumer.process_message(message)
            if result is not None:
                processed.append(result)

    logger.info("Time to process: %ss" % (time.time() - time_start))

    time_write = time.time()
    consumer.flush_batch(processed)

    logger.info("Time to write: %ss" % (time.time() - time_write))
    logger.info("Time total: %ss" % (time.time() - time_start))
    logger.info("Number of events processed: %s" % len(processed))
