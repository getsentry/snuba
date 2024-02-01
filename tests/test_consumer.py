import functools
import itertools
import json
import pickle
from datetime import datetime
from pickle import PickleBuffer
from typing import MutableSequence
from unittest.mock import Mock, call

import pytest
from arroyo.backends.kafka import KafkaPayload
from arroyo.types import BrokerValue, Message, Partition, Topic
from py._path.local import LocalPath

from snuba.clusters.cluster import ClickhouseClientSettings
from snuba.consumers.consumer import (
    BytesInsertBatch,
    InsertBatchWriter,
    LatencyRecorder,
    ProcessedMessageBatchWriter,
    ReplacementBatchWriter,
    build_batch_writer,
    process_message,
)
from snuba.consumers.strategy_factory import KafkaConsumerStrategyFactory
from snuba.datasets.schemas.tables import TableSchema
from snuba.datasets.storage import Storage
from snuba.datasets.storages.factory import get_storage, get_writable_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.processor import InsertBatch, ReplacementBatch
from snuba.utils.metrics.wrapper import MetricsWrapper
from snuba.utils.streams.topics import Topic as SnubaTopic
from tests.assertions import assert_changes
from tests.backends.metrics import TestingMetricsBackend, Timing
from tests.fixtures import get_raw_error_message


def test_streaming_consumer_strategy(tmpdir: LocalPath) -> None:
    messages = (
        Message(
            BrokerValue(
                KafkaPayload(
                    None, json.dumps(get_raw_error_message()).encode("utf-8"), []
                ),
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

    health_check_file = tmpdir / "health.txt"
    factory = KafkaConsumerStrategyFactory(
        None,
        functools.partial(
            process_message, processor, "consumer_group", SnubaTopic.EVENTS, True
        ),
        write_step,
        max_batch_size=10,
        max_batch_time=60,
        max_insert_batch_size=None,
        max_insert_batch_time=None,
        processes=None,
        input_block_size=None,
        output_block_size=None,
        health_check_file=health_check_file.strpath,
        metrics_tags={},
    )

    commit_function = Mock()
    strategy = factory.create_with_partitions(commit_function, {})

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

    assert health_check_file.check()


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
def test_metrics_writing_e2e() -> None:
    distributions_storage = get_storage(StorageKey.METRICS_DISTRIBUTIONS)
    polymorphic_bucket = get_writable_storage(StorageKey.METRICS_RAW)
    dist_message = json.dumps(
        {
            "org_id": 1,
            "project_id": 2,
            "name": "sentry.transactions.transaction.duration",
            "unit": "ms",
            "type": "d",
            "value": [24.0, 80.0, 119.0, 146.0, 182.0],
            "timestamp": int(datetime.now().timestamp()),
            "tags": {"6": 91, "9": 134, "4": 117, "5": 7},
            "metric_id": 8,
            "retention_days": 90,
            "sentry_received_timestamp": datetime.now().timestamp(),
            "use_case_id": "performance",
            "mapping_meta": {},
        }
    )

    commit = Mock()

    table_writer = polymorphic_bucket.get_table_writer()
    stream_loader = table_writer.get_stream_loader()

    metrics = TestingMetricsBackend()

    strategy = KafkaConsumerStrategyFactory(
        None,
        functools.partial(
            process_message,
            stream_loader.get_processor(),
            "consumer_group",
            SnubaTopic.METRICS,
            True,
        ),
        build_batch_writer(table_writer, metrics=metrics),
        max_batch_size=10,
        max_batch_time=60,
        max_insert_batch_size=None,
        max_insert_batch_time=None,
        processes=None,
        input_block_size=None,
        output_block_size=None,
        health_check_file=None,
        metrics_tags={},
    ).create_with_partitions(commit, {})

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
                call({}),
                call({Partition(Topic("topic"), 0): 1}),
                call({}, force=True),
            ],
        ):
            strategy.close()
            strategy.join()


def test_latency_recorder() -> None:
    recorder = LatencyRecorder()

    assert recorder.max_ms is None

    recorder.record(1.0)
    recorder.record(0.5)
    recorder.record(1.2)

    assert recorder.max_ms == 1200.0
    # (2.7 / 3) * 1000 == 900
    assert recorder.avg_ms == 900.0
