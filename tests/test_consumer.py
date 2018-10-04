import calendar
import re
from datetime import datetime, timedelta
from mock import patch, MagicMock
import pytz
import simplejson as json
import time
from datadog import statsd
import six
from six.moves import range
from confluent_kafka import TopicPartition

from base import BaseTest

from snuba.consumer import AbstractBatchWorker, BatchingKafkaConsumer, ConsumerWorker
from snuba.processor import PAYLOAD_DATETIME_FORMAT, CLICKHOUSE_DATETIME_FORMAT


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


class FakeKafkaProducer(object):
    def __init__(self):
        self.messages = []
        self._callbacks = []

    def poll(self, *args, **kwargs):
        while self._callbacks:
            callback, message = self._callbacks.pop()
            callback(None, message)
        return 0

    def produce(self, topic, value, key=None, on_delivery=None):
        message = FakeKafkaMessage(
            topic=topic,
            partition=None,  # XXX: the partition is unknown (depends on librdkafka)
            offset=None,  # XXX: the offset is unknown (depends on state)
            key=key,
            value=value,
        )
        self.messages.append(message)
        if on_delivery is not None:
            self._callbacks.append((on_delivery, message))


class FakeKafkaConsumer(object):
    def __init__(self):
        self.items = []
        self.commit_calls = 0
        self.close_calls = 0
        self.positions = {}

    def poll(self, *args, **kwargs):
        try:
            message = self.items.pop(0)
        except IndexError:
            return None

        self.positions[(message.topic(), message.partition())] = message.offset() + 1

        return message

    def commit(self, *args, **kwargs):
        self.commit_calls += 1
        return [
            TopicPartition(topic, partition, offset)
            for (topic, partition), offset in
            six.iteritems(self.positions)
        ]

    def close(self, *args, **kwargs):
        self.close_calls += 1


class FakeBatchingKafkaConsumer(BatchingKafkaConsumer):
    def create_consumer(self, *args, **kwargs):
        return FakeKafkaConsumer()

    def create_producer(self, *args, **kwargs):
        return FakeKafkaProducer()


class FakeWorker(AbstractBatchWorker):
    def __init__(self, *args, **kwargs):
        super(FakeWorker, self).__init__(*args, **kwargs)
        self.processed = []
        self.flushed = []
        self.shutdown_calls = 0

    def process_message(self, message):
        self.processed.append(message.value())
        return message.value()

    def flush_batch(self, batch):
        self.flushed.append(batch)

    def shutdown(self):
        self.shutdown_calls += 1


class TestConsumer(BaseTest):
    def test_batch_size(self):
        consumer = FakeBatchingKafkaConsumer(
            'topic',
            worker=FakeWorker(),
            max_batch_size=2,
            max_batch_time=100,
            metrics=statsd,
            bootstrap_servers=None,
            group_id='group',
            commit_log_topic='commits',
        )

        consumer.consumer.items = [FakeKafkaMessage('topic', 0, i, i) for i in [1, 2, 3]]
        for x in range(len(consumer.consumer.items)):
            consumer._run_once()
        consumer._shutdown()

        assert consumer.worker.processed == [1, 2, 3]
        assert consumer.worker.flushed == [[1, 2]]
        assert consumer.worker.shutdown_calls == 1
        assert consumer.consumer.commit_calls == 1
        assert consumer.consumer.close_calls == 1

        assert len(consumer.producer.messages) == 1
        commit_message = consumer.producer.messages[0]
        assert commit_message.topic() == 'commits'
        assert commit_message.key() == '{}:{}:{}'.format('topic', 0, 'group').encode('utf-8')
        assert commit_message.value() == '{}'.format(2 + 1).encode('utf-8')  # offsets are last processed message offset + 1

    @patch('time.time')
    def test_batch_time(self, mock_time):
        consumer = FakeBatchingKafkaConsumer(
            'topic',
            worker=FakeWorker(),
            max_batch_size=100,
            max_batch_time=2000,
            metrics=statsd,
            bootstrap_servers=None,
            group_id='group',
            commit_log_topic='commits',
        )

        mock_time.return_value = time.mktime(datetime(2018, 1, 1, 0, 0, 0).timetuple())
        consumer.consumer.items = [FakeKafkaMessage('topic', 0, i, i) for i in [1, 2, 3]]
        for x in range(len(consumer.consumer.items)):
            consumer._run_once()

        mock_time.return_value = time.mktime(datetime(2018, 1, 1, 0, 0, 1).timetuple())
        consumer.consumer.items = [FakeKafkaMessage('topic', 0, i, i) for i in [4, 5, 6]]
        for x in range(len(consumer.consumer.items)):
            consumer._run_once()

        mock_time.return_value = time.mktime(datetime(2018, 1, 1, 0, 0, 5).timetuple())
        consumer.consumer.items = [FakeKafkaMessage('topic', 0, i, i) for i in [7, 8, 9]]
        for x in range(len(consumer.consumer.items)):
            consumer._run_once()

        consumer._shutdown()

        assert consumer.worker.processed == [1, 2, 3, 4, 5, 6, 7, 8, 9]
        assert consumer.worker.flushed == [[1, 2, 3, 4, 5, 6]]
        assert consumer.worker.shutdown_calls == 1
        assert consumer.consumer.commit_calls == 1
        assert consumer.consumer.close_calls == 1

        assert len(consumer.producer.messages) == 1
        commit_message = consumer.producer.messages[0]
        assert commit_message.topic() == 'commits'
        assert commit_message.key() == '{}:{}:{}'.format('topic', 0, 'group').encode('utf-8')
        assert commit_message.value() == '{}'.format(6 + 1).encode('utf-8')  # offsets are last processed message offset + 1

    def test_offsets(self):
        event = self.event

        class FakeMessage(object):
            def value(self):
                # event doesn't really matter
                return json.dumps((0, 'insert', event))

            def offset(self):
                return 123

            def partition(self):
                return 456

        test_worker = ConsumerWorker(self.clickhouse, self.table, self.table)
        batch = [test_worker.process_message(FakeMessage())]
        test_worker.flush_batch(batch)

        assert self.clickhouse.execute(
            "SELECT project_id, event_id, offset, partition FROM %s" % self.table
        ) == [(self.event['project_id'], self.event['event_id'], 123, 456)]

    def test_skip_too_old(self):
        test_worker = ConsumerWorker(self.clickhouse, self.table, self.table)

        event = self.event
        old_timestamp = datetime.utcnow() - timedelta(days=300)
        old_timestamp_str = old_timestamp.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        event['datetime'] = old_timestamp_str
        event['data']['datetime'] = old_timestamp_str
        event['data']['received'] = int(calendar.timegm(old_timestamp.timetuple()))

        class FakeMessage(object):
            def value(self):
                return json.dumps((0, 'insert', event))

        assert test_worker.process_message(FakeMessage()) is None

    def _await_true(self, query, test_fn, timeout=5.0):
        """ALTERs in ClickHouse run async, so to keep tests fast we run the
        assert query repeatedly for N seconds and exit early if it True,
        otherwise exit False."""

        started = time.time()
        while (time.time() - started) < timeout:
            resp = self.clickhouse.execute(query)
            if test_fn(resp):
                return True
            time.sleep(0.1)
        return False

    def test_delete_groups(self):
        self.event['project_id'] = 1
        self.event['group_id'] = 1
        self.write_raw_events(self.event)

        group_count_query = """
            SELECT count()
            FROM %s
            WHERE project_id = 1
            AND group_id = 1
            AND deleted = 0
        """ % self.table

        assert self._await_true(group_count_query, lambda resp: resp[0][0] == 1)

        timestamp = datetime.now(tz=pytz.utc)
        test_worker = ConsumerWorker(self.clickhouse, self.table, self.table)

        class FakeMessage(object):
            def value(self):
                return json.dumps((0, 'delete_groups', {
                    'project_id': 1,
                    'group_ids': [1],
                    'datetime': timestamp.strftime(PAYLOAD_DATETIME_FORMAT),
                }))

        processed = test_worker.process_message(FakeMessage())
        test_worker.flush_batch([processed])

        assert self._await_true(group_count_query, lambda resp: resp[0][0] == 0)

    # def test_unmerge(self):
    #     self.event['project_id'] = 1
    #     self.event['group_id'] = 1
    #     self.event['event_id'] = 'a' * 32
    #     self.write_raw_events(self.event)

    #     base_query = """
    #         SELECT count()
    #         FROM %s
    #         WHERE project_id = 1
    #         AND group_id = %s
    #         AND deleted = 0
    #     """

    #     group1_count_query = base_query % (self.table, 1)
    #     group2_count_query = base_query % (self.table, 2)

    #     assert self._await_true(group1_count_query, lambda resp: resp[0][0] == 1)
    #     assert self._await_true(group2_count_query, lambda resp: resp[0][0] == 0)

    #     timestamp = datetime.now(tz=pytz.utc)
    #     test_worker = ConsumerWorker(self.clickhouse, self.table, self.table)

    #     class FakeMessage(object):
    #         def value(self):
    #             return json.dumps((0, 'unmerge', {
    #                 'project_id': 1,
    #                 'new_group_id': 2,
    #                 'event_ids': ['a' * 32],
    #                 'datetime': timestamp.strftime(PAYLOAD_DATETIME_FORMAT),
    #             }))

    #     processed = test_worker.process_message(FakeMessage())
    #     test_worker.flush_batch([processed])

    #     assert self._await_true(group1_count_query, lambda resp: resp[0][0] == 0)
    #     assert self._await_true(group2_count_query, lambda resp: resp[0][0] == 1)

    def test_merge(self):
        self.event['project_id'] = 1
        self.event['group_id'] = 1
        self.write_raw_events(self.event)

        base_query = """
            SELECT count()
            FROM %s
            WHERE project_id = 1
            AND group_id = %s
            AND deleted = 0
        """

        group1_count_query = base_query % (self.table, 1)
        group2_count_query = base_query % (self.table, 2)

        assert self._await_true(group1_count_query, lambda resp: resp[0][0] == 1)
        assert self._await_true(group2_count_query, lambda resp: resp[0][0] == 0)

        timestamp = datetime.now(tz=pytz.utc)
        test_worker = ConsumerWorker(self.clickhouse, self.table, self.table)

        class FakeMessage(object):
            def value(self):
                return json.dumps((0, 'merge', {
                    'project_id': 1,
                    'new_group_id': 2,
                    'previous_group_id': 1,
                    'datetime': timestamp.strftime(PAYLOAD_DATETIME_FORMAT),
                }))

        processed = test_worker.process_message(FakeMessage())
        test_worker.flush_batch([processed])

        assert self._await_true(group1_count_query, lambda resp: resp[0][0] == 0)
        assert self._await_true(group2_count_query, lambda resp: resp[0][0] == 1)
