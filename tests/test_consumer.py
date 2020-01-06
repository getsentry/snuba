import calendar
from datetime import datetime, timedelta
import simplejson as json

from snuba.consumer import ConsumerWorker
from snuba.datasets.factory import enforce_table_writer
from snuba.processor import ProcessedMessage, ProcessorAction
from snuba.utils.metrics.backends.dummy import DummyMetricsBackend
from snuba.utils.streams.kafka import KafkaPayload
from snuba.utils.streams.types import Message, Partition, Topic
from tests.base import BaseEventsTest
from tests.backends.confluent_kafka import FakeConfluentKafkaProducer


class TestConsumer(BaseEventsTest):

    metrics = DummyMetricsBackend()

    def test_offsets(self):
        event = self.event

        message: Message[KafkaPayload] = Message(
            Partition(Topic("events"), 456),
            123,
            KafkaPayload(
                None, json.dumps((0, "insert", event)).encode("utf-8")
            ),  # event doesn't really matter
            datetime.now(),
        )

        replacement_topic = (
            enforce_table_writer(self.dataset)
            .get_stream_loader()
            .get_replacement_topic_spec()
        )
        test_worker = ConsumerWorker(
            self.dataset,
            FakeConfluentKafkaProducer(),
            replacement_topic.topic_name,
            self.metrics,
        )
        batch = [test_worker.process_message(message)]
        test_worker.flush_batch(batch)

        assert self.clickhouse.execute(
            "SELECT project_id, event_id, offset, partition FROM %s" % self.table
        ) == [(self.event["project_id"], self.event["event_id"], 123, 456)]

    def test_skip_too_old(self):
        replacement_topic = (
            enforce_table_writer(self.dataset)
            .get_stream_loader()
            .get_replacement_topic_spec()
        )
        test_worker = ConsumerWorker(
            self.dataset,
            FakeConfluentKafkaProducer(),
            replacement_topic.topic_name,
            self.metrics,
        )

        event = self.event
        old_timestamp = datetime.utcnow() - timedelta(days=300)
        old_timestamp_str = old_timestamp.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        event["datetime"] = old_timestamp_str
        event["data"]["datetime"] = old_timestamp_str
        event["data"]["received"] = int(calendar.timegm(old_timestamp.timetuple()))

        message: Message[KafkaPayload] = Message(
            Partition(Topic("events"), 1),
            42,
            KafkaPayload(None, json.dumps((0, "insert", event)).encode("utf-8")),
            datetime.now(),
        )

        assert test_worker.process_message(message) is None

    def test_produce_replacement_messages(self):
        producer = FakeConfluentKafkaProducer()
        replacement_topic = (
            enforce_table_writer(self.dataset)
            .get_stream_loader()
            .get_replacement_topic_spec()
        )
        test_worker = ConsumerWorker(
            self.dataset, producer, replacement_topic.topic_name, self.metrics
        )

        test_worker.flush_batch(
            [
                ProcessedMessage(
                    action=ProcessorAction.REPLACE, data=[("1", {"project_id": 1})],
                ),
                ProcessedMessage(
                    action=ProcessorAction.REPLACE, data=[("2", {"project_id": 2})],
                ),
            ]
        )

        assert [(m._topic, m._key, m._value) for m in producer.messages] == [
            ("event-replacements", b"1", b'{"project_id": 1}'),
            ("event-replacements", b"2", b'{"project_id": 2}'),
        ]
