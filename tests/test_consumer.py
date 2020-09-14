import calendar
import itertools
import pickle
from datetime import datetime, timedelta
from pickle import PickleBuffer
from typing import MutableSequence
from unittest.mock import Mock

import pytest
import simplejson as json

from snuba.clusters.cluster import ClickhouseClientSettings
from snuba.consumer import (
    ConsumerWorker,
    JSONRowInsertBatch,
    StreamingConsumerStrategyFactory,
)
from snuba.datasets.factory import enforce_table_writer
from snuba.datasets.storages import StorageKey
from snuba.datasets.storages.factory import get_storage
from snuba.processor import InsertBatch, ReplacementBatch
from snuba.utils.metrics.backends.dummy import DummyMetricsBackend
from snuba.utils.streams import Message, Partition, Topic
from snuba.utils.streams.backends.kafka import KafkaPayload
from tests.assertions import assert_changes
from tests.backends.confluent_kafka import FakeConfluentKafkaProducer
from tests.backends.metrics import TestingMetricsBackend, Timing
from tests.base import BaseEventsTest


class TestConsumer(BaseEventsTest):

    metrics = DummyMetricsBackend()

    def test_offsets(self):
        event = self.event

        message: Message[KafkaPayload] = Message(
            Partition(Topic("events"), 456),
            123,
            KafkaPayload(
                None, json.dumps((2, "insert", event)).encode("utf-8")
            ),  # event doesn't really matter
            datetime.now(),
        )

        test_worker = ConsumerWorker(
            self.dataset.get_writable_storage(),
            producer=FakeConfluentKafkaProducer(),
            replacements_topic=Topic(
                enforce_table_writer(self.dataset)
                .get_stream_loader()
                .get_replacement_topic_spec()
                .topic_name
            ),
            metrics=self.metrics,
        )
        batch = [test_worker.process_message(message)]
        test_worker.flush_batch(batch)

        clickhouse = (
            get_storage(StorageKey.EVENTS)
            .get_cluster()
            .get_query_connection(ClickhouseClientSettings.QUERY)
        )

        assert clickhouse.execute(
            "SELECT project_id, event_id, offset, partition FROM %s" % self.table
        ) == [(self.event["project_id"], self.event["event_id"], 123, 456)]

    def test_skip_too_old(self):
        test_worker = ConsumerWorker(
            self.dataset.get_writable_storage(),
            producer=FakeConfluentKafkaProducer(),
            replacements_topic=Topic(
                enforce_table_writer(self.dataset)
                .get_stream_loader()
                .get_replacement_topic_spec()
                .topic_name
            ),
            metrics=self.metrics,
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
            KafkaPayload(None, json.dumps((2, "insert", event)).encode("utf-8")),
            datetime.now(),
        )

        assert test_worker.process_message(message) is None

    def test_produce_replacement_messages(self):
        producer = FakeConfluentKafkaProducer()
        test_worker = ConsumerWorker(
            self.dataset.get_writable_storage(),
            producer=producer,
            replacements_topic=Topic(
                enforce_table_writer(self.dataset)
                .get_stream_loader()
                .get_replacement_topic_spec()
                .topic_name
            ),
            metrics=self.metrics,
        )

        test_worker.flush_batch(
            [
                ReplacementBatch("1", [{"project_id": 1}]),
                ReplacementBatch("2", [{"project_id": 2}]),
            ]
        )

        assert [(m._topic, m._key, m._value) for m in producer.messages] == [
            ("event-replacements", b"1", b'{"project_id":1}'),
            ("event-replacements", b"2", b'{"project_id":2}'),
        ]


def test_streaming_consumer_strategy() -> None:
    messages = (
        Message(
            Partition(Topic("events"), 0),
            i,
            KafkaPayload(None, b"{}", None),
            datetime.now(),
        )
        for i in itertools.count()
    )

    replacements_producer = FakeConfluentKafkaProducer()

    processor = Mock()
    processor.process_message.side_effect = [
        None,
        InsertBatch([{}]),
        ReplacementBatch("key", [{}]),
    ]

    writer = Mock()

    metrics = TestingMetricsBackend()

    factory = StreamingConsumerStrategyFactory(
        None,
        processor,
        writer,
        metrics,
        max_batch_size=10,
        max_batch_time=60,
        processes=None,
        input_block_size=None,
        output_block_size=None,
        replacements_producer=replacements_producer,
        replacements_topic=Topic("replacements"),
    )

    commit_function = Mock()
    strategy = factory.create(commit_function)

    for i in range(3):
        strategy.poll()
        strategy.submit(next(messages))

    assert metrics.calls == []

    processor.process_message.side_effect = [{}]

    with pytest.raises(TypeError):
        strategy.poll()
        strategy.submit(next(messages))

    def get_number_of_insertion_metrics() -> int:
        count = 0
        for call in metrics.calls:
            if isinstance(call, Timing) and call.name == "insertions.latency_ms":
                count += 1
        return count

    expected_write_count = 1

    with assert_changes(
        get_number_of_insertion_metrics, 0, expected_write_count
    ), assert_changes(
        lambda: writer.write.call_count, 0, expected_write_count
    ), assert_changes(
        lambda: len(replacements_producer.messages), 0, 1
    ):
        strategy.close()
        strategy.join()


def test_json_row_batch_pickle_simple() -> None:
    batch = JSONRowInsertBatch([b"foo", b"bar", b"baz"])
    assert pickle.loads(pickle.dumps(batch)) == batch


def test_json_row_batch_pickle_out_of_band() -> None:
    batch = JSONRowInsertBatch([b"foo", b"bar", b"baz"])

    buffers: MutableSequence[PickleBuffer] = []
    data = pickle.dumps(batch, protocol=5, buffer_callback=buffers.append)
    assert pickle.loads(data, buffers=[b.raw() for b in buffers]) == batch
