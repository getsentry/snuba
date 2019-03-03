import calendar
from datadog import statsd
from datetime import datetime, timedelta
from mock import patch
import pytest
import simplejson as json
import time
import six
from six.moves import range

from base import (
    BaseTest,
    FakeBatchingKafkaConsumer,
    FakeWorker,
    FakeKafkaMessage,
    FakeKafkaProducer,
)

from snuba.consumer import (
    ConsumerWorker,
    InvalidMessageType,
    InvalidMessageVersion,
    INSERT,
    REPLACE,
)


class TestConsumer(BaseTest):
    def setup_method(self, test_method):
        super(TestConsumer, self).setup_method(test_method)

        self.test_worker = ConsumerWorker(
            self.clickhouse, self.dataset, FakeKafkaProducer(), 'topic'
        )

    def test_simple_version_0(self):
        action, processed = ConsumerWorker.validate_message((0, 'insert', self.event))

        assert action == INSERT
        for field in ('event_id', 'project_id', 'message', 'platform'):
            assert processed[field] == self.event[field]

    def test_simple_version_1(self):
        assert ConsumerWorker.validate_message(
            (0, 'insert', self.event)
        ) == ConsumerWorker.validate_message((1, 'insert', self.event, {}))

    def test_invalid_type_version_0(self):
        with pytest.raises(InvalidMessageType):
            ConsumerWorker.validate_message((0, 'invalid', self.event))

    def test_invalid_version(self):
        with pytest.raises(InvalidMessageVersion):
            ConsumerWorker.validate_message((2 ** 32 - 1, 'insert', self.event))

    def test_invalid_format(self):
        with pytest.raises(InvalidMessageVersion):
            ConsumerWorker.validate_message((-1, 'insert', self.event))

    def test_v1_delete_groups_skipped(self):
        assert ConsumerWorker.validate_message((1, 'delete_groups', {})) is None

    def test_v1_merge_skipped(self):
        assert ConsumerWorker.validate_message((1, 'merge', {})) is None

    def test_v1_unmerge_skipped(self):
        assert ConsumerWorker.validate_message((1, 'unmerge', {})) is None

    def test_v2_invalid_type(self):
        with pytest.raises(InvalidMessageType):
            ConsumerWorker.validate_message((2, '__invalid__', {}))

    def test_v2_start_delete_groups(self):
        project_id = 1
        message = (2, 'start_delete_groups', {'project_id': project_id})
        assert ConsumerWorker.validate_message(message) == (
            REPLACE,
            (six.text_type(project_id), message),
        )

    def test_v2_end_delete_groups(self):
        project_id = 1
        message = (2, 'end_delete_groups', {'project_id': project_id})
        assert ConsumerWorker.validate_message(message) == (
            REPLACE,
            (six.text_type(project_id), message),
        )

    def test_v2_start_merge(self):
        project_id = 1
        message = (2, 'start_merge', {'project_id': project_id})
        assert ConsumerWorker.validate_message(message) == (
            REPLACE,
            (six.text_type(project_id), message),
        )

    def test_v2_end_merge(self):
        project_id = 1
        message = (2, 'end_merge', {'project_id': project_id})
        assert ConsumerWorker.validate_message(message) == (
            REPLACE,
            (six.text_type(project_id), message),
        )

    def test_v2_start_unmerge(self):
        project_id = 1
        message = (2, 'start_unmerge', {'project_id': project_id})
        assert ConsumerWorker.validate_message(message) == (
            REPLACE,
            (six.text_type(project_id), message),
        )

    def test_v2_end_unmerge(self):
        project_id = 1
        message = (2, 'end_unmerge', {'project_id': project_id})
        assert ConsumerWorker.validate_message(message) == (
            REPLACE,
            (six.text_type(project_id), message),
        )

    def test_v2_start_delete_tag(self):
        project_id = 1
        message = (2, 'start_delete_tag', {'project_id': project_id})
        assert ConsumerWorker.validate_message(message) == (
            REPLACE,
            (six.text_type(project_id), message),
        )

    def test_v2_end_delete_tag(self):
        project_id = 1
        message = (2, 'end_delete_tag', {'project_id': project_id})
        assert ConsumerWorker.validate_message(message) == (
            REPLACE,
            (six.text_type(project_id), message),
        )

    def test_simple(self):
        _, processed = ConsumerWorker.validate_message(self.event)

        for field in ('event_id', 'project_id', 'message', 'platform'):
            assert processed[field] == self.event[field]

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
            producer=FakeKafkaProducer(),
        )

        consumer.consumer.items = [
            FakeKafkaMessage('topic', 0, i, i) for i in [1, 2, 3]
        ]
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
        assert commit_message.key() == '{}:{}:{}'.format('topic', 0, 'group').encode(
            'utf-8'
        )
        assert commit_message.value() == '{}'.format(2 + 1).encode(
            'utf-8'
        )  # offsets are last processed message offset + 1

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
            producer=FakeKafkaProducer(),
        )

        mock_time.return_value = time.mktime(datetime(2018, 1, 1, 0, 0, 0).timetuple())
        consumer.consumer.items = [
            FakeKafkaMessage('topic', 0, i, i) for i in [1, 2, 3]
        ]
        for x in range(len(consumer.consumer.items)):
            consumer._run_once()

        mock_time.return_value = time.mktime(datetime(2018, 1, 1, 0, 0, 1).timetuple())
        consumer.consumer.items = [
            FakeKafkaMessage('topic', 0, i, i) for i in [4, 5, 6]
        ]
        for x in range(len(consumer.consumer.items)):
            consumer._run_once()

        mock_time.return_value = time.mktime(datetime(2018, 1, 1, 0, 0, 5).timetuple())
        consumer.consumer.items = [
            FakeKafkaMessage('topic', 0, i, i) for i in [7, 8, 9]
        ]
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
        assert commit_message.key() == '{}:{}:{}'.format('topic', 0, 'group').encode(
            'utf-8'
        )
        assert commit_message.value() == '{}'.format(6 + 1).encode(
            'utf-8'
        )  # offsets are last processed message offset + 1

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

        batch = [self.test_worker.process_message(FakeMessage())]
        self.test_worker.flush_batch(batch)

        assert self.clickhouse.execute(
            "SELECT project_id, event_id, offset, partition FROM %s"
            % self.dataset.SCHEMA.QUERY_TABLE
        ) == [(self.event['project_id'], self.event['event_id'], 123, 456)]

    def test_skip_too_old(self):
        event = self.event
        old_timestamp = datetime.utcnow() - timedelta(days=300)
        old_timestamp_str = old_timestamp.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        event['datetime'] = old_timestamp_str
        event['data']['datetime'] = old_timestamp_str
        event['data']['received'] = int(calendar.timegm(old_timestamp.timetuple()))

        class FakeMessage(object):
            def value(self):
                return json.dumps((0, 'insert', event))

            def offset(self):
                return 123

            def partition(self):
                return 456

        assert self.test_worker.process_message(FakeMessage()) is None

    def test_produce_replacement_messages(self):
        topic = 'topic'

        self.test_worker.flush_batch(
            [(REPLACE, ('1', {'project_id': 1})), (REPLACE, ('2', {'project_id': 2}))]
        )

        assert [
            (m._topic, m._key, m._value) for m in self.test_worker.producer.messages
        ] == [
            ('topic', b'1', b'{"project_id": 1}'),
            ('topic', b'2', b'{"project_id": 2}'),
        ]

    def test_dead_letter_topic(self):
        class FailingFakeWorker(FakeWorker):
            def process_message(*args, **kwargs):
                1 / 0

        producer = FakeKafkaProducer()
        consumer = FakeBatchingKafkaConsumer(
            'topic',
            worker=FailingFakeWorker(),
            max_batch_size=100,
            max_batch_time=2000,
            metrics=statsd,
            bootstrap_servers=None,
            group_id='group',
            producer=producer,
            dead_letter_topic='dlt',
        )

        message = FakeKafkaMessage(
            'topic', partition=1, offset=2, key='key', value='value'
        )
        consumer.consumer.items = [message]
        consumer._run_once()

        assert len(producer.messages) == 1
        produced_message = producer.messages[0]

        assert ('dlt', message.key(), message.value()) == (
            produced_message.topic(),
            produced_message.key(),
            produced_message.value(),
        )

        assert produced_message.headers() == {
            'partition': '1',
            'offset': '2',
            'topic': 'topic',
        }
