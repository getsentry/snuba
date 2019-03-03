import cProfile
import logging
import six
import time
from itertools import chain

from snuba.util import settings_override


logger = logging.getLogger('snuba.perf')


class FakeKafkaMessage(object):
    def __init__(self, topic, partition, offset, value, key=None, headers=None, error=None):
        self._topic = topic
        self._partition = partition
        self._offset = offset
        self._value = value
        self._key = key
        self._headers = {
            six.text_type(k): six.text_type(v) if v else None
            for k, v in six.iteritems(headers)
        } if headers else None
        self._headers = headers
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

    def headers(self):
        return self._headers

    def error(self):
        return self._error


def get_messages(events_file):
    "Create a FakeKafkaMessage for each JSON event in the file."
    messages = []
    raw_events = open(events_file).readlines()
    for raw_event in raw_events:
        messages.append(FakeKafkaMessage('events', 1, 0, raw_event))
    return messages


def run(events_file, clickhouse, dataset, repeat=1,
        profile_process=False, profile_write=False):
    from snuba.consumer import ConsumerWorker

    clickhouse.execute(dataset.SCHEMA.get_local_table_definition())

    consumer = ConsumerWorker(
        clickhouse=clickhouse,
        dataset=dataset,
        producer=None,
        replacements_topic=None,
    )

    messages = get_messages(events_file)
    messages = chain(*([messages] * repeat))
    processed = []

    def process():
        with settings_override({'DISCARD_OLD_EVENTS': False}):
            for message in messages:
                result = consumer.process_message(message)
                if result is not None:
                    processed.append(result)

    def write():
        consumer.flush_batch(processed)

    time_start = time.time()
    if profile_process:
        cProfile.runctx('process()', globals(), locals(), sort='cumulative')
    else:
        process()
    time_write = time.time()
    if profile_write:
        cProfile.runctx('write()', globals(), locals(), sort='cumulative')
    else:
        write()
    time_finish = time.time()

    format_time = lambda t: ("%.2f" % t).rjust(10, ' ')

    time_to_process = (time_write - time_start) * 1000
    time_to_write = (time_finish - time_write) * 1000
    time_total = (time_finish - time_start) * 1000
    num_events = len(processed)

    logger.info("Number of events: %s" % six.text_type(num_events).rjust(10, ' '))
    logger.info("Total:            %sms" % format_time(time_total))
    logger.info("Total process:    %sms" % format_time(time_to_process))
    logger.info("Total write:      %sms" % format_time(time_to_write))
    logger.info("Process event:    %sms/ea" % format_time(time_to_process / num_events))
    logger.info("Write event:      %sms/ea" % format_time(time_to_write / num_events))
