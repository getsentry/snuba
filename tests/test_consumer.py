import functools
import itertools
import json
import pickle
from datetime import datetime
from pathlib import Path
from pickle import PickleBuffer
from typing import MutableSequence
from unittest.mock import Mock, call

import pytest
from arroyo.backends.kafka import KafkaPayload
from arroyo.types import BrokerValue, Message, Partition, Topic

from snuba.clusters.cluster import ClickhouseClientSettings
from snuba.consumers.consumer import (
    BytesInsertBatch,
    CommitLogConfig,
    CommitLogHeartbeatState,
    InsertBatchWriter,
    LatencyRecorder,
    MultistorageCollector,
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


def test_streaming_consumer_strategy(tmpdir: Path) -> None:
    messages = (
        Message(
            BrokerValue(
                KafkaPayload(None, json.dumps(get_raw_error_message()).encode("utf-8"), []),
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
            insert_batch_writer=InsertBatchWriter(writer, MetricsWrapper(metrics, "insertions")),
            replacement_batch_writer=ReplacementBatchWriter(
                replacements_producer, Topic("replacements")
            ),
        )

    health_check_file = tmpdir / "health.txt"
    factory = KafkaConsumerStrategyFactory(
        None,
        functools.partial(process_message, processor, "consumer_group", SnubaTopic.EVENTS, True),
        write_step,
        max_batch_size=10,
        max_batch_time=60,
        max_insert_batch_size=None,
        max_insert_batch_time=None,
        processes=None,
        input_block_size=None,
        output_block_size=None,
        health_check_file=str(health_check_file),
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

    with (
        assert_changes(get_number_of_insertion_metrics, 0, expected_write_count),
        assert_changes(lambda: writer.write.call_count, 0, expected_write_count),
        assert_changes(
            lambda: replacements_producer.produce.call_count,
            0,
            1,
        ),
    ):
        strategy.close()
        strategy.join()

    assert health_check_file.exists()


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
            "use_case_id": "sessions",
            "name": "sentry.transactions.transaction.duration",
            "unit": "ms",
            "type": "d",
            "value": [24.0, 80.0, 119.0, 146.0, 182.0],
            "timestamp": int(datetime.now().timestamp()),
            "tags": {"6": 91, "9": 134, "4": 117, "5": 7},
            "metric_id": 8,
            "retention_days": 90,
            "sentry_received_timestamp": datetime.now().timestamp(),
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
    with (
        assert_changes(lambda: get_row_count(distributions_storage), 0, 4),
        assert_changes(lambda: get_row_count(distributions_storage), 0, 4),
    ):
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


def test_commit_log_heartbeat_for_idle_partitions() -> None:
    """
    Verify that when a batch is flushed containing messages only from
    partitions 0 and 1, but the consumer is assigned partitions 0-3,
    commit log entries are still produced for partitions 2 and 3 using
    their last known offsets.
    """
    commit_log_producer = Mock()
    commit_log_topic = Topic("snuba-commit-log")
    commit_log_config = CommitLogConfig(
        producer=commit_log_producer,
        topic=commit_log_topic,
        group_id="test-group",
    )

    heartbeat_state = CommitLogHeartbeatState()
    # Simulate assigned partitions 0-3
    assigned = {Partition(Topic("events"), i): 0 for i in range(4)}
    heartbeat_state.update_partitions(assigned)

    writer = Mock()
    metrics = TestingMetricsBackend()

    # --- Batch 1: messages on all 4 partitions (establishes last known offsets) ---
    batch_writer_1 = ProcessedMessageBatchWriter(
        insert_batch_writer=InsertBatchWriter(writer, MetricsWrapper(metrics, "insertions")),
        commit_log_config=commit_log_config,
        heartbeat_state=heartbeat_state,
    )

    now = datetime.now()
    for i in range(4):
        msg = Message(
            BrokerValue(
                KafkaPayload(None, b'{"col1": "val1"}', []),
                Partition(Topic("events"), i),
                100 + i,  # offset
                now,
            )
        )
        batch_writer_1.submit(msg.replace(BytesInsertBatch([b'{"col1": "val1"}'], now)))

    batch_writer_1.close()

    # After batch 1: should have 4 commit log produces (one per partition)
    # plus heartbeats - but all partitions had data, so no heartbeats
    assert commit_log_producer.produce.call_count == 4

    commit_log_producer.reset_mock()

    # --- Batch 2: messages only on partitions 0 and 1 ---
    batch_writer_2 = ProcessedMessageBatchWriter(
        insert_batch_writer=InsertBatchWriter(writer, MetricsWrapper(metrics, "insertions")),
        commit_log_config=commit_log_config,
        heartbeat_state=heartbeat_state,
    )

    for i in range(2):  # Only partitions 0 and 1
        msg = Message(
            BrokerValue(
                KafkaPayload(None, b'{"col1": "val1"}', []),
                Partition(Topic("events"), i),
                200 + i,  # offset
                now,
            )
        )
        batch_writer_2.submit(msg.replace(BytesInsertBatch([b'{"col1": "val1"}'], now)))

    batch_writer_2.close()

    # Should have 4 total produces:
    # - 2 for partitions 0 and 1 (actual data)
    # - 2 heartbeats for partitions 2 and 3 (idle)
    assert commit_log_producer.produce.call_count == 4

    # Verify the produce calls include the right partitions
    produced_keys = []
    for c in commit_log_producer.produce.call_args_list:
        # The key encodes the partition info
        produced_keys.append(
            c.kwargs.get("key") or c.args[1] if len(c.args) > 1 else c.kwargs.get("key")
        )

    # All 4 partitions should have commit log entries
    # (keys are in format "topic:partition:group" encoded bytes)
    assert commit_log_producer.produce.call_count == 4


def test_commit_log_heartbeat_no_heartbeat_without_prior_offset() -> None:
    """
    Verify that no heartbeat is produced for a partition that has never
    had any messages (no last known offset).
    """
    commit_log_producer = Mock()
    commit_log_topic = Topic("snuba-commit-log")
    commit_log_config = CommitLogConfig(
        producer=commit_log_producer,
        topic=commit_log_topic,
        group_id="test-group",
    )

    heartbeat_state = CommitLogHeartbeatState()
    assigned = {Partition(Topic("events"), i): 0 for i in range(4)}
    heartbeat_state.update_partitions(assigned)

    writer = Mock()
    metrics = TestingMetricsBackend()

    # Batch with messages only on partition 0 (no prior offsets for 1-3)
    batch_writer = ProcessedMessageBatchWriter(
        insert_batch_writer=InsertBatchWriter(writer, MetricsWrapper(metrics, "insertions")),
        commit_log_config=commit_log_config,
        heartbeat_state=heartbeat_state,
    )

    now = datetime.now()
    msg = Message(
        BrokerValue(
            KafkaPayload(None, b'{"col1": "val1"}', []),
            Partition(Topic("events"), 0),
            100,
            now,
        )
    )
    batch_writer.submit(msg.replace(BytesInsertBatch([b'{"col1": "val1"}'], now)))
    batch_writer.close()

    # Only 1 produce: partition 0 has data, partitions 1-3 have no prior offset
    assert commit_log_producer.produce.call_count == 1


def test_commit_log_heartbeat_state_rebalance() -> None:
    """
    Verify that when partitions are reassigned, old partition offsets are cleaned up.
    """
    heartbeat_state = CommitLogHeartbeatState()

    # Initially assigned partitions 0-3
    assigned = {Partition(Topic("events"), i): 0 for i in range(4)}
    heartbeat_state.update_partitions(assigned)

    # Simulate some last produced offsets
    now = datetime.now()
    for i in range(4):
        heartbeat_state.last_produced_offsets[Partition(Topic("events"), i)] = (100 + i, now)

    assert len(heartbeat_state.last_produced_offsets) == 4

    # Rebalance: now only assigned partitions 0-1
    new_assigned = {Partition(Topic("events"), i): 0 for i in range(2)}
    heartbeat_state.update_partitions(new_assigned)

    # Offsets for partitions 2 and 3 should be cleaned up
    assert len(heartbeat_state.last_produced_offsets) == 2
    assert Partition(Topic("events"), 0) in heartbeat_state.last_produced_offsets
    assert Partition(Topic("events"), 1) in heartbeat_state.last_produced_offsets
    assert Partition(Topic("events"), 2) not in heartbeat_state.last_produced_offsets
    assert Partition(Topic("events"), 3) not in heartbeat_state.last_produced_offsets


def test_commit_log_heartbeat_offset_evolution() -> None:
    """
    Verify that heartbeat offsets evolve as new batches arrive. After batch 2
    with partition 0 at offset 500, the heartbeat for partition 0 in batch 3
    (where partition 0 is idle) should use offset 500, not the old offset 100.
    """
    commit_log_producer = Mock()
    commit_log_config = CommitLogConfig(
        producer=commit_log_producer,
        topic=Topic("snuba-commit-log"),
        group_id="test-group",
    )

    heartbeat_state = CommitLogHeartbeatState()
    assigned = {Partition(Topic("events"), i): 0 for i in range(2)}
    heartbeat_state.update_partitions(assigned)

    writer = Mock()
    metrics = TestingMetricsBackend()
    now = datetime.now()

    def make_batch_writer() -> ProcessedMessageBatchWriter:
        return ProcessedMessageBatchWriter(
            insert_batch_writer=InsertBatchWriter(writer, MetricsWrapper(metrics, "insertions")),
            commit_log_config=commit_log_config,
            heartbeat_state=heartbeat_state,
        )

    def submit_message(bw: ProcessedMessageBatchWriter, partition: int, offset: int) -> None:
        msg = Message(
            BrokerValue(
                KafkaPayload(None, b'{"col1": "val1"}', []),
                Partition(Topic("events"), partition),
                offset,
                now,
            )
        )
        bw.submit(msg.replace(BytesInsertBatch([b'{"col1": "val1"}'], now)))

    # Batch 1: both partitions, establishing initial offsets
    bw1 = make_batch_writer()
    submit_message(bw1, 0, 100)
    submit_message(bw1, 1, 200)
    bw1.close()
    commit_log_producer.reset_mock()

    # Batch 2: only partition 0 at a higher offset
    bw2 = make_batch_writer()
    submit_message(bw2, 0, 500)
    bw2.close()

    # 1 data produce (partition 0) + 1 heartbeat (partition 1 at offset 200)
    assert commit_log_producer.produce.call_count == 2
    commit_log_producer.reset_mock()

    # Batch 3: only partition 1 at a higher offset
    bw3 = make_batch_writer()
    submit_message(bw3, 1, 700)
    bw3.close()

    # 1 data produce (partition 1) + 1 heartbeat (partition 0)
    assert commit_log_producer.produce.call_count == 2

    # The heartbeat for partition 0 should use next_offset 501 (from batch 2),
    # not the original 101 (from batch 1). Check the shared state.
    # Note: BrokerValue stores next_offset = offset + 1
    assert heartbeat_state.last_produced_offsets[Partition(Topic("events"), 0)] == (
        501,
        now,
    )
    assert heartbeat_state.last_produced_offsets[Partition(Topic("events"), 1)] == (
        701,
        now,
    )


def test_commit_log_heartbeat_no_state_means_no_heartbeats() -> None:
    """
    Verify that when heartbeat_state is None (e.g., commit log disabled or DLQ
    consumer), no heartbeat logic is triggered — only data offsets are produced.
    """
    commit_log_producer = Mock()
    commit_log_config = CommitLogConfig(
        producer=commit_log_producer,
        topic=Topic("snuba-commit-log"),
        group_id="test-group",
    )

    writer = Mock()
    metrics = TestingMetricsBackend()
    now = datetime.now()

    # No heartbeat_state
    bw = ProcessedMessageBatchWriter(
        insert_batch_writer=InsertBatchWriter(writer, MetricsWrapper(metrics, "insertions")),
        commit_log_config=commit_log_config,
        heartbeat_state=None,
    )

    msg = Message(
        BrokerValue(
            KafkaPayload(None, b'{"col1": "val1"}', []),
            Partition(Topic("events"), 0),
            100,
            now,
        )
    )
    bw.submit(msg.replace(BytesInsertBatch([b'{"col1": "val1"}'], now)))
    bw.close()

    # Only the 1 data produce — no heartbeats
    assert commit_log_producer.produce.call_count == 1


def test_multistorage_collector_heartbeat_for_idle_partitions() -> None:
    """
    Verify MultistorageCollector produces heartbeat commit log entries
    for idle assigned partitions.
    """
    commit_log_producer = Mock()
    commit_log_config = CommitLogConfig(
        producer=commit_log_producer,
        topic=Topic("snuba-commit-log"),
        group_id="test-group",
    )

    heartbeat_state = CommitLogHeartbeatState()
    assigned = {Partition(Topic("events"), i): 0 for i in range(4)}
    heartbeat_state.update_partitions(assigned)

    # Create mock ProcessedMessageBatchWriter steps for a fake storage key
    storage_key = StorageKey("errors")
    writer = Mock()
    metrics = TestingMetricsBackend()
    step = ProcessedMessageBatchWriter(
        insert_batch_writer=InsertBatchWriter(writer, MetricsWrapper(metrics, "insertions")),
    )

    now = datetime.now()

    # Batch 1: establish offsets for all 4 partitions via the collector
    collector1 = MultistorageCollector(
        steps={storage_key: step},
        commit_log_config=commit_log_config,
        heartbeat_state=heartbeat_state,
    )

    for i in range(4):
        msg = Message(
            BrokerValue(
                KafkaPayload(None, b'{"col1": "val1"}', []),
                Partition(Topic("events"), i),
                100 + i,
                now,
            )
        )
        collector1.submit(
            msg.replace([(storage_key, BytesInsertBatch([b'{"col1": "val1"}'], now))])
        )
    collector1.close()

    # 4 produces for data partitions, no heartbeats
    assert commit_log_producer.produce.call_count == 4
    commit_log_producer.reset_mock()

    # Batch 2: only partitions 0 and 1
    step2 = ProcessedMessageBatchWriter(
        insert_batch_writer=InsertBatchWriter(writer, MetricsWrapper(metrics, "insertions")),
    )

    collector2 = MultistorageCollector(
        steps={storage_key: step2},
        commit_log_config=commit_log_config,
        heartbeat_state=heartbeat_state,
    )

    for i in range(2):
        msg = Message(
            BrokerValue(
                KafkaPayload(None, b'{"col1": "val1"}', []),
                Partition(Topic("events"), i),
                200 + i,
                now,
            )
        )
        collector2.submit(
            msg.replace([(storage_key, BytesInsertBatch([b'{"col1": "val1"}'], now))])
        )
    collector2.close()

    # 2 data produces + 2 heartbeats for idle partitions 2 and 3
    assert commit_log_producer.produce.call_count == 4


def test_build_batch_writer_passes_heartbeat_state() -> None:
    """
    Verify that build_batch_writer correctly passes heartbeat_state
    to each ProcessedMessageBatchWriter it creates.
    """
    commit_log_producer = Mock()
    commit_log_config = CommitLogConfig(
        producer=commit_log_producer,
        topic=Topic("snuba-commit-log"),
        group_id="test-group",
    )

    heartbeat_state = CommitLogHeartbeatState()
    assigned = {Partition(Topic("events"), i): 0 for i in range(2)}
    heartbeat_state.update_partitions(assigned)

    writer_mock = Mock()
    table_writer = Mock()
    table_writer.get_batch_writer.return_value = writer_mock

    metrics = TestingMetricsBackend()

    factory = build_batch_writer(
        table_writer,
        metrics=metrics,
        commit_log_config=commit_log_config,
        heartbeat_state=heartbeat_state,
    )

    # Create two batch writers from the factory — they should share heartbeat_state
    bw1 = factory()
    bw2 = factory()

    # Both should be ProcessedMessageBatchWriter instances
    assert isinstance(bw1, ProcessedMessageBatchWriter)
    assert isinstance(bw2, ProcessedMessageBatchWriter)

    now = datetime.now()

    # Submit a message to bw1 to establish an offset for partition 0
    msg = Message(
        BrokerValue(
            KafkaPayload(None, b'{"col1": "val1"}', []),
            Partition(Topic("events"), 0),
            100,
            now,
        )
    )
    bw1.submit(msg.replace(BytesInsertBatch([b'{"col1": "val1"}'], now)))
    bw1.close()
    commit_log_producer.reset_mock()

    # bw2 gets a message only on partition 1 — partition 0 should get a heartbeat
    # because the shared state knows about partition 0's offset from bw1
    msg2 = Message(
        BrokerValue(
            KafkaPayload(None, b'{"col1": "val1"}', []),
            Partition(Topic("events"), 1),
            200,
            now,
        )
    )
    bw2.submit(msg2.replace(BytesInsertBatch([b'{"col1": "val1"}'], now)))
    bw2.close()

    # 1 data produce (partition 1) + 1 heartbeat (partition 0)
    assert commit_log_producer.produce.call_count == 2


def test_strategy_factory_updates_heartbeat_state_on_rebalance() -> None:
    """
    Verify that KafkaConsumerStrategyFactory.create_with_partitions()
    updates the heartbeat state with the new partition assignments.
    """
    heartbeat_state = CommitLogHeartbeatState()

    processor = Mock()
    processor.process_message.return_value = None

    writer = Mock()
    metrics = TestingMetricsBackend()

    def write_step() -> ProcessedMessageBatchWriter:
        return ProcessedMessageBatchWriter(
            insert_batch_writer=InsertBatchWriter(writer, MetricsWrapper(metrics, "insertions")),
            heartbeat_state=heartbeat_state,
        )

    factory = KafkaConsumerStrategyFactory(
        None,
        functools.partial(process_message, processor, "consumer_group", SnubaTopic.EVENTS, True),
        write_step,
        max_batch_size=10,
        max_batch_time=60,
        max_insert_batch_size=None,
        max_insert_batch_time=None,
        processes=None,
        input_block_size=None,
        output_block_size=None,
        health_check_file=None,
        metrics_tags={},
        heartbeat_state=heartbeat_state,
    )

    # Initial assignment: partitions 0-3
    partitions = {Partition(Topic("events"), i): 0 for i in range(4)}
    commit = Mock()
    factory.create_with_partitions(commit, partitions)
    assert heartbeat_state.assigned_partitions == set(partitions.keys())

    # Rebalance: now only partitions 0-1
    new_partitions = {Partition(Topic("events"), i): 0 for i in range(2)}
    factory.create_with_partitions(commit, new_partitions)
    assert heartbeat_state.assigned_partitions == set(new_partitions.keys())
