import cProfile
import logging
import os
import tempfile
import time
from itertools import chain

from snuba.util import settings_override
from snuba.utils.metrics.backends.dummy import DummyMetricsBackend
from snuba.utils.streams.consumers.backends.kafka import KafkaMessage, TopicPartition


logger = logging.getLogger('snuba.perf')


def get_messages(events_file):
    "Create a fake Kafka message for each JSON event in the file."
    messages = []
    raw_events = open(events_file).readlines()
    for raw_event in raw_events:
        messages.append(
            KafkaMessage(
                TopicPartition('events', 1),
                0,
                raw_event.encode('utf-8')
            ),
        )
    return messages


def run(events_file, dataset, repeat=1,
        profile_process=False, profile_write=False):
    """
    Measures the write performance of a dataset
    """

    from snuba.consumer import ConsumerWorker
    from snuba.clickhouse.native import ClickhousePool

    for statement in dataset.get_dataset_schemas().get_create_statements():
        ClickhousePool().execute(statement)

    consumer = ConsumerWorker(
        dataset=dataset,
        producer=None,
        replacements_topic=None,
        metrics=DummyMetricsBackend(),
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
        filename = tempfile.NamedTemporaryFile(
            prefix=os.path.basename(events_file) + '.process.',
            suffix='.pstats',
            delete=False,
        ).name
        cProfile.runctx('process()', globals(), locals(), filename=filename)
        logger.info('Profile Data: %s', filename)
    else:
        process()
    time_write = time.time()
    if profile_write:
        filename = tempfile.NamedTemporaryFile(
            prefix=os.path.basename(events_file) + '.write.',
            suffix='.pstats',
            delete=False,
        ).name
        cProfile.runctx('write()', globals(), locals(), filename=filename)
        logger.info('Profile Data: %s', filename)
    else:
        write()
    time_finish = time.time()

    format_time = lambda t: ("%.2f" % t).rjust(10, ' ')

    time_to_process = (time_write - time_start) * 1000
    time_to_write = (time_finish - time_write) * 1000
    time_total = (time_finish - time_start) * 1000
    num_events = len(processed)

    logger.info("Number of events: %s" % str(num_events).rjust(10, ' '))
    logger.info("Total:            %sms" % format_time(time_total))
    logger.info("Total process:    %sms" % format_time(time_to_process))
    logger.info("Total write:      %sms" % format_time(time_to_write))
    logger.info("Process event:    %sms/ea" % format_time(time_to_process / num_events))
    logger.info("Write event:      %sms/ea" % format_time(time_to_write / num_events))
