import itertools
import json
import pickle
from datetime import datetime
from pickle import PickleBuffer
from typing import MutableSequence
from unittest.mock import Mock

import pytest

from snuba.consumer import (
    MultistorageConsumerProcessingStrategyFactory,
    JSONRowInsertBatch,
    StreamingConsumerStrategyFactory,
)
from snuba.processor import InsertBatch, ReplacementBatch
from snuba.utils.streams import Message, Partition, Topic
from snuba.utils.streams.backends.kafka import KafkaPayload
from tests.assertions import assert_changes
from tests.backends.confluent_kafka import FakeConfluentKafkaProducer
from tests.backends.metrics import TestingMetricsBackend, Timing


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


def test_multiprocess_strategy() -> None:
    from snuba.datasets.storages import groupassignees, groupedmessages
    from tests.datasets.cdc.test_groupassignee import TestGroupassignee
    from tests.datasets.cdc.test_groupedmessage import TestGroupedMessage

    commit = Mock()

    strategy = MultistorageConsumerProcessingStrategyFactory(
        [groupassignees.storage, groupedmessages.storage],
        10,
        10,
        TestingMetricsBackend(),
    ).create(commit)

    payloads = [
        KafkaPayload(None, b"{}", [("table", b"ignored")]),
        KafkaPayload(
            None,
            json.dumps(TestGroupassignee.INSERT_MSG),
            [("table", b"sentry_groupedassignee")],
        ),
        KafkaPayload(
            None,
            json.dumps(TestGroupedMessage.INSERT_MSG),
            [("table", b"sentry_groupedmessage")],
        ),
    ]

    messages = [
        Message(
            Partition(Topic("topic"), 0), offset, payload, datetime.now(), offset + 1
        )
        for offset, payload in enumerate(payloads)
    ]

    for message in messages:
        strategy.submit(message)

    strategy.close()
    strategy.join()
