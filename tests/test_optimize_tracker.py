import uuid
from datetime import datetime, timedelta

import pytest

from snuba import optimize, settings
from snuba.clusters.cluster import ClickhouseClientSettings
from snuba.datasets.storage import WritableTableStorage
from snuba.datasets.storages import StorageKey
from snuba.datasets.storages.factory import get_writable_storage
from snuba.optimize import run_optimize_cron_job
from snuba.optimize_tracker import OptimizedPartitionTracker
from snuba.processor import InsertBatch
from snuba.redis import redis_client
from tests.helpers import write_processed_messages


@pytest.mark.parametrize(
    "tracker",
    [
        pytest.param(
            OptimizedPartitionTracker(
                redis_client=redis_client,
                host="some-hostname.domain.com",
                database="some-database",
                table="some-table",
                expire_time=(datetime.now() + timedelta(minutes=3)),
            ),
            id="redis",
        ),
    ],
)
def test_optimized_partition_tracker(tracker: OptimizedPartitionTracker) -> None:
    assert tracker.get_all_parts() is None
    assert tracker.get_completed_parts() is None
    assert tracker.get_parts_to_optimize() is None

    tracker.update_all_parts(["Partition 1", "Partition 2"])
    tracker.update_completed_parts("Partition 1")
    assert tracker.get_completed_parts() == {"Partition 1"}
    assert tracker.get_parts_to_optimize() == {"Partition 2"}

    tracker.update_completed_parts("Partition 2")
    assert tracker.get_completed_parts() == {"Partition 1", "Partition 2"}
    assert tracker.get_parts_to_optimize() is not None
    assert len(tracker.get_parts_to_optimize()) == 0

    tracker.delete_all_states()
    assert tracker.get_all_parts() is None
    assert tracker.get_completed_parts() is None


def test_run_optimize_with_partition_tracker() -> None:
    def write_error_message(writable_storage: WritableTableStorage, time: int) -> None:
        write_processed_messages(
            writable_storage,
            [
                InsertBatch(
                    [
                        {
                            "event_id": str(uuid.uuid4()),
                            "project_id": 1,
                            "deleted": 0,
                            "timestamp": time,
                            "retention_days": settings.DEFAULT_RETENTION_DAYS,
                        }
                    ],
                    None,
                ),
            ],
        )

    storage = get_writable_storage(StorageKey.ERRORS)
    cluster = storage.get_cluster()
    clickhouse_pool = cluster.get_query_connection(ClickhouseClientSettings.OPTIMIZE)
    table = storage.get_table_writer().get_schema().get_local_table_name()
    database = cluster.get_database()
    tracker = OptimizedPartitionTracker(
        redis_client=redis_client,
        host="localhost",
        database=database,
        table=table,
        expire_time=(datetime.now() + timedelta(minutes=3)),
    )

    # Write some messages to the database
    for week in range(0, 4):
        write_error_message(
            writable_storage=storage,
            time=int((datetime.now() - timedelta(weeks=week)).timestamp()),
        )
        write_error_message(
            writable_storage=storage,
            time=int((datetime.now() - timedelta(weeks=week)).timestamp()),
        )

    parts = optimize.get_partitions_to_optimize(
        clickhouse_pool, storage, database, table
    )

    original_num_parts = len(parts)
    assert original_num_parts > 0
    assert tracker.get_all_parts() is None
    assert tracker.get_completed_parts() is None

    # Mark the parts as optimized in partition tracker to test behavior.
    tracker.update_all_parts([part.name for part in parts])
    for part in parts:
        tracker.update_all_parts([part.name])
        tracker.update_completed_parts(part.name)

    tracker_completed_partitions = tracker.get_completed_parts()
    assert tracker_completed_partitions is not None
    assert len(tracker_completed_partitions) == original_num_parts

    num_optimized = run_optimize_cron_job(
        clickhouse=clickhouse_pool,
        storage=storage,
        database=database,
        parallel=1,
        clickhouse_host="localhost",
        tracker=tracker,
    )
    assert num_optimized == 0

    # Fix the optimized partition tracker and run_optimize again.
    # Now we should optimize all the partitions.
    tracker.delete_all_states()
    num_optimized = run_optimize_cron_job(
        clickhouse=clickhouse_pool,
        storage=storage,
        database=database,
        parallel=1,
        clickhouse_host="localhost",
        tracker=tracker,
    )
    assert num_optimized == original_num_parts
