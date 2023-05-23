import functools
import itertools
import json
import pickle
from datetime import datetime
from pickle import PickleBuffer
from typing import MutableSequence, Optional
from unittest.mock import Mock, call

import pytest
from arroyo.backends.kafka import KafkaPayload
from arroyo.types import BrokerValue, Message, Partition, Topic

from snuba.clusters.cluster import ClickhouseClientSettings
from snuba.consumers.consumer import (
    BytesInsertBatch,
    InsertBatchWriter,
    LatencyRecorder,
    MultistorageConsumerProcessingStrategyFactory,
    ProcessedMessageBatchWriter,
    ReplacementBatchWriter,
    process_message,
)
from snuba.consumers.strategy_factory import KafkaConsumerStrategyFactory
from snuba.datasets.schemas.tables import TableSchema
from snuba.datasets.storage import Storage
from snuba.datasets.storages.factory import get_cdc_storage, get_writable_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.processor import InsertBatch, ReplacementBatch
from snuba.utils.metrics.wrapper import MetricsWrapper
from snuba.utils.streams.topics import Topic as SnubaTopic
from tests.assertions import assert_changes
from tests.backends.metrics import TestingMetricsBackend, Timing


def test_streaming_consumer_strategy() -> None:
    messages = (
        Message(
            BrokerValue(
                KafkaPayload(None, b"{}", []),
                Partition(Topic("events"), 0),
                i,
                datetime.now(),
            )
        )
        for i in itertools.count()
    )

    replacements_producer = Mock()

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
        functools.partial(
            process_message, processor, "consumer_group", SnubaTopic.EVENTS
        ),
        write_step,
        max_batch_size=10,
        max_batch_time=60,
        processes=None,
        input_block_size=None,
        output_block_size=None,
    )

    commit_function = Mock()
    partitions = Mock()
    strategy = factory.create_with_partitions(commit_function, partitions)

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
        lambda: replacements_producer.produce.call_count,
        0,
        1,
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


@pytest.mark.clickhouse_db
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
    from tests.datasets.cdc.test_groupassignee import TestGroupassignee
    from tests.datasets.cdc.test_groupedmessage import TestGroupedMessage

    groupassignees_storage = get_cdc_storage(StorageKey.GROUPASSIGNEES)
    groupedmessages_storage = get_cdc_storage(StorageKey.GROUPEDMESSAGES)

    commit = Mock()
    partitions = Mock()

    storages = [groupassignees_storage, groupedmessages_storage]

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
    ).create_with_partitions(commit, partitions)

    payloads = [
        KafkaPayload(None, b"{}", [("table", b"ignored")]),
        KafkaPayload(
            None,
            json.dumps(TestGroupassignee.INSERT_MSG).encode("utf8"),
            [("table", groupassignees_storage.get_postgres_table().encode("utf8"))],
        ),
        KafkaPayload(
            None,
            json.dumps(TestGroupedMessage.INSERT_MSG).encode("utf8"),
            [("table", groupedmessages_storage.get_postgres_table().encode("utf8"))],
        ),
    ]

    now = datetime.now()

    messages = [
        Message(BrokerValue(payload, Partition(Topic("topic"), 0), offset, now))
        for offset, payload in enumerate(payloads)
    ]

    with assert_changes(
        lambda: get_row_count(groupedmessages_storage), 0, 1
    ), assert_changes(lambda: get_row_count(groupedmessages_storage), 0, 1):

        for message in messages:
            strategy.submit(message)

        with assert_changes(
            lambda: commit.call_args_list,
            [],
            [
                call({Partition(topic=Topic(name="topic"), index=0): 3}),
                call({}, force=True),
            ],
        ):
            strategy.close()
            strategy.join()


@pytest.mark.clickhouse_db
def test_metrics_writing_e2e() -> None:
    distributions_storage = get_writable_storage(StorageKey.METRICS_DISTRIBUTIONS)
    polymorphic_bucket = get_writable_storage(StorageKey.METRICS_RAW)
    dist_message = json.dumps(
        {
            "org_id": 1,
            "project_id": 2,
            "name": "sentry.transactions.transaction.duration",
            "unit": "ms",
            "type": "d",
            "value": [24.0, 80.0, 119.0, 146.0, 182.0],
            "timestamp": datetime.now().timestamp(),
            "tags": {"6": 91, "9": 134, "4": 117, "5": 7},
            "metric_id": 8,
            "retention_days": 90,
            "sentry_received_timestamp": datetime.now().timestamp(),
        }
    )

    commit = Mock()
    partitions = Mock()

    storages = [polymorphic_bucket]

    strategy = MultistorageConsumerProcessingStrategyFactory(
        storages, 10, 10, None, None, None, TestingMetricsBackend(), None, None
    ).create_with_partitions(commit, partitions)

    payloads = [KafkaPayload(None, dist_message.encode("utf-8"), [])]
    now = datetime.now()
    messages = [
        Message(BrokerValue(payload, Partition(Topic("topic"), 0), offset, now))
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
            [
                call({Partition(Topic("topic"), 0): 1}),
                call({}, force=True),
            ],
        ):
            strategy.close()
            strategy.join()


def test_latency_recorder() -> None:
    recorder = LatencyRecorder()

    assert recorder.has_data() == False

    recorder.record(1.0)
    recorder.record(0.5)
    recorder.record(1.2)

    assert recorder.has_data() == True
    assert recorder.max_ms == 1200.0
    # (2.7 / 3) * 1000 == 900
    assert recorder.avg_ms == 900.0
