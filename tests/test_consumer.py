import functools
import itertools
import json
import pickle
from datetime import datetime
from pickle import PickleBuffer
from typing import MutableSequence, Optional
from unittest.mock import MagicMock, Mock, call, patch

import pytest
from arroyo import Message, Partition, Topic
from arroyo.backends.kafka import KafkaPayload
from arroyo.backends.local.backend import LocalBroker as Broker
from arroyo.backends.local.storages.memory import MemoryMessageStorage
from arroyo.processing.strategies.streaming import KafkaConsumerStrategyFactory
from arroyo.types import Position
from arroyo.utils.clock import TestingClock

from snuba import settings
from snuba.clickhouse.errors import ClickhouseWriterError
from snuba.clusters.cluster import ClickhouseClientSettings
from snuba.consumers.consumer import (
    BytesInsertBatch,
    InsertBatchWriter,
    MultistorageConsumerProcessingStrategyFactory,
    ProcessedMessageBatchWriter,
    ReplacementBatchWriter,
    process_message,
)
from snuba.datasets.schemas.tables import TableSchema
from snuba.datasets.storage import Storage
from snuba.datasets.storages.factory import get_writable_storage
from snuba.processor import InsertBatch, ReplacementBatch
from snuba.utils.metrics.wrapper import MetricsWrapper
from tests.assertions import assert_changes
from tests.backends.confluent_kafka import FakeConfluentKafkaProducer
from tests.backends.metrics import TestingMetricsBackend, Timing
from tests.fixtures import get_raw_event, get_raw_transaction


def test_streaming_consumer_strategy() -> None:
    messages = (
        Message(
            Partition(Topic("events"), 0),
            i,
            KafkaPayload(None, b"{}", []),
            datetime.now(),
        )
        for i in itertools.count()
    )

    replacements_producer = FakeConfluentKafkaProducer()

    processor = Mock()
    processor.process_message.side_effect = [
        None,
        InsertBatch([{}], None),
        ReplacementBatch("key", [{}]),
    ]

    writer = Mock()

    metrics = TestingMetricsBackend()

    def write_step() -> ProcessedMessageBatchWriter:
        return ProcessedMessageBatchWriter(
            insert_batch_writer=InsertBatchWriter(
                writer, MetricsWrapper(metrics, "insertions")
            ),
            replacement_batch_writer=ReplacementBatchWriter(
                replacements_producer, Topic("replacements")
            ),
        )

    factory = KafkaConsumerStrategyFactory(
        None,
        functools.partial(process_message, processor),
        write_step,
        max_batch_size=10,
        max_batch_time=60,
        processes=None,
        input_block_size=None,
        output_block_size=None,
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
        for c in metrics.calls:
            if isinstance(c, Timing) and c.name == "insertions.latency_ms":
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
    batch = BytesInsertBatch([b"foo", b"bar", b"baz"], datetime(2021, 1, 1, 11, 0, 1))
    assert pickle.loads(pickle.dumps(batch)) == batch


def test_json_row_batch_pickle_out_of_band() -> None:
    batch = BytesInsertBatch([b"foo", b"bar", b"baz"], datetime(2021, 1, 1, 11, 0, 1))

    buffers: MutableSequence[PickleBuffer] = []
    data = pickle.dumps(batch, protocol=5, buffer_callback=buffers.append)
    assert pickle.loads(data, buffers=[b.raw() for b in buffers]) == batch


def get_row_count(storage: Storage) -> int:
    schema = storage.get_schema()
    assert isinstance(schema, TableSchema)

    return int(
        storage.get_cluster()
        .get_query_connection(ClickhouseClientSettings.INSERT)
        .execute(f"SELECT count() FROM {schema.get_local_table_name()}")
        .results[0][0]
    )


@pytest.mark.parametrize(
    "processes, input_block_size, output_block_size",
    [
        pytest.param(1, int(32 * 1e6), int(64 * 1e6), id="multiprocessing"),
        pytest.param(None, None, None, id="no multiprocessing"),
    ],
)
def test_multistorage_strategy(
    processes: Optional[int],
    input_block_size: Optional[int],
    output_block_size: Optional[int],
) -> None:
    from snuba.datasets.storages import groupassignees, groupedmessages
    from tests.datasets.cdc.test_groupassignee import TestGroupassignee
    from tests.datasets.cdc.test_groupedmessage import TestGroupedMessage

    commit = Mock()

    storages = [groupassignees.storage, groupedmessages.storage]

    strategy = MultistorageConsumerProcessingStrategyFactory(
        storages,
        10,
        10,
        processes,
        input_block_size,
        output_block_size,
        TestingMetricsBackend(),
        None,
        None,
    ).create(commit)

    payloads = [
        KafkaPayload(None, b"{}", [("table", b"ignored")]),
        KafkaPayload(
            None,
            json.dumps(TestGroupassignee.INSERT_MSG).encode("utf8"),
            [("table", groupassignees.storage.get_postgres_table().encode("utf8"))],
        ),
        KafkaPayload(
            None,
            json.dumps(TestGroupedMessage.INSERT_MSG).encode("utf8"),
            [("table", groupedmessages.storage.get_postgres_table().encode("utf8"))],
        ),
    ]

    now = datetime.now()

    messages = [
        Message(Partition(Topic("topic"), 0), offset, payload, now, offset + 1)
        for offset, payload in enumerate(payloads)
    ]

    with assert_changes(
        lambda: get_row_count(groupassignees.storage), 0, 1
    ), assert_changes(lambda: get_row_count(groupedmessages.storage), 0, 1):

        for message in messages:
            strategy.submit(message)

        with assert_changes(
            lambda: commit.call_args_list,
            [],
            [call({Partition(Topic("topic"), 0): Position(3, now)})],
        ):
            strategy.close()
            strategy.join()


@patch("snuba.consumers.consumer.InsertBatchWriter.close")
@patch("snuba.consumers.consumer.DeadLetterStep.submit")
@pytest.mark.parametrize(
    "processes, input_block_size, output_block_size",
    [
        pytest.param(1, int(32 * 1e6), int(64 * 1e6), id="multiprocessing"),
        pytest.param(None, None, None, id="no multiprocessing"),
    ],
)
def test_multistorage_strategy_dead_letter_step(
    mock_submit: MagicMock,
    mock_close: MagicMock,
    processes: Optional[int],
    input_block_size: Optional[int],
    output_block_size: Optional[int],
) -> None:
    from snuba.datasets.storages import StorageKey

    clock = TestingClock()
    broker: Broker[KafkaPayload] = Broker(MemoryMessageStorage(), clock)

    topic = Topic("test-dlq")

    broker.create_topic(topic, partitions=1)

    producer = broker.get_producer()

    # we want to raise a dummy clickhousewritererror so that we cause
    # the multistorage collector to use the dead letter step
    mock_close.side_effect = ClickhouseWriterError("oops", code=500, row=None)

    commit = Mock()
    storage_keys = [StorageKey.ERRORS_V2, StorageKey.TRANSACTIONS_V2]

    storages = [get_writable_storage(key) for key in storage_keys]

    strategy = MultistorageConsumerProcessingStrategyFactory(
        storages,
        10,
        10,
        processes,
        input_block_size,
        output_block_size,
        TestingMetricsBackend(),
        producer,
        topic,
    ).create(commit)

    payloads = [
        KafkaPayload(
            None,
            json.dumps((2, "insert", get_raw_event(),)).encode("utf8"),
            [("transaction_forwarder", "1".encode("utf8"))],
        ),
        KafkaPayload(
            None,
            json.dumps((2, "insert", get_raw_transaction(),)).encode("utf8"),
            [("transaction_forwarder", "0".encode("utf8"))],
        ),
    ]

    now = datetime.now()

    messages = [
        Message(Partition(Topic("topic"), 0), offset, payload, now, offset + 1)
        for offset, payload in enumerate(payloads)
    ]

    for message in messages:
        strategy.submit(message)

    strategy.close()
    strategy.join()

    # assert that we submit to the dead letter step for both messages
    assert len(mock_submit.call_args_list) == 2


def test_metrics_writing_e2e() -> None:
    from snuba.datasets.storages.metrics import (
        counters_storage,
        distributions_storage,
        sets_storage,
    )

    settings.WRITE_METRICS_AGG_DIRECTLY = True

    dist_message = """
        {
            "org_id":1,
            "project_id":2,
            "name":"sentry.transactions.transaction.duration",
            "unit":"ms",
            "type":"d",
            "value":[24.0,80.0,119.0,146.0,182.0],
            "timestamp":1641418510,
            "tags":{"6":91,"9":134,"4":117,"5":7},
            "metric_id":8,
            "retention_days":90
        }
    """

    commit = Mock()

    storages = [
        distributions_storage,
        sets_storage,
        counters_storage,
    ]

    strategy = MultistorageConsumerProcessingStrategyFactory(
        storages, 10, 10, None, None, None, TestingMetricsBackend(), None, None
    ).create(commit)

    payloads = [KafkaPayload(None, dist_message.encode("utf-8"), [])]
    now = datetime.now()
    messages = [
        Message(Partition(Topic("topic"), 0), offset, payload, now, offset + 1)
        for offset, payload in enumerate(payloads)
    ]

    # 4 rows written, one for each metrics granularity
    with assert_changes(
        lambda: get_row_count(distributions_storage), 0, 4
    ), assert_changes(lambda: get_row_count(distributions_storage), 0, 4):
        for message in messages:
            strategy.submit(message)

        with assert_changes(
            lambda: commit.call_args_list,
            [],
            [call({Partition(Topic("topic"), 0): Position(1, now)})],
        ):
            strategy.close()
            strategy.join()

    settings.WRITE_METRICS_AGG_DIRECTLY = False
