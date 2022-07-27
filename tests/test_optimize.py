import uuid
from datetime import datetime, timedelta
from typing import Callable, Mapping

import pytest
from freezegun import freeze_time

from snuba import optimize, settings
from snuba.clusters.cluster import ClickhouseClientSettings
from snuba.datasets.storages import StorageKey
from snuba.datasets.storages.factory import get_writable_storage
from snuba.optimize import _get_metrics_tags, optimize_partition_runner
from snuba.optimize_scheduler import OptimizedSchedulerTimeout, OptimizeScheduler
from snuba.optimize_tracker import OptimizedPartitionTracker
from snuba.processor import InsertBatch
from snuba.redis import redis_client
from tests.helpers import write_processed_messages

last_midnight = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)


test_data = [
    pytest.param(
        StorageKey.ERRORS,
        lambda dt: InsertBatch(
            [
                {
                    "event_id": str(uuid.uuid4()),
                    "project_id": 1,
                    "group_id": 1,
                    "deleted": 0,
                    "timestamp": dt,
                    "retention_days": settings.DEFAULT_RETENTION_DAYS,
                }
            ],
            None,
        ),
        last_midnight + timedelta(minutes=30),
        id="errors non-parallel",
    ),
    pytest.param(
        StorageKey.ERRORS,
        lambda dt: InsertBatch(
            [
                {
                    "event_id": str(uuid.uuid4()),
                    "project_id": 1,
                    "group_id": 1,
                    "deleted": 0,
                    "timestamp": dt,
                    "retention_days": settings.DEFAULT_RETENTION_DAYS,
                }
            ],
            None,
        ),
        last_midnight
        + settings.PARALLEL_OPTIMIZE_JOB_START_TIME
        + timedelta(minutes=30),
        id="errors parallel",
    ),
    pytest.param(
        StorageKey.TRANSACTIONS,
        lambda dt: InsertBatch(
            [
                {
                    "event_id": str(uuid.uuid4()),
                    "project_id": 1,
                    "deleted": 0,
                    "finish_ts": dt,
                    "retention_days": settings.DEFAULT_RETENTION_DAYS,
                }
            ],
            None,
        ),
        last_midnight + timedelta(minutes=30),
        id="transactions non-parallel",
    ),
    pytest.param(
        StorageKey.TRANSACTIONS,
        lambda dt: InsertBatch(
            [
                {
                    "event_id": str(uuid.uuid4()),
                    "project_id": 1,
                    "deleted": 0,
                    "finish_ts": dt,
                    "retention_days": settings.DEFAULT_RETENTION_DAYS,
                }
            ],
            None,
        ),
        last_midnight
        + settings.PARALLEL_OPTIMIZE_JOB_START_TIME
        + timedelta(minutes=30),
        id="transactions parallel",
    ),
]


class TestOptimize:
    @pytest.mark.parametrize(
        "storage_key, create_event_row_for_date, current_time",
        test_data,
    )
    def test_optimize(
        self,
        storage_key: StorageKey,
        create_event_row_for_date: Callable[[datetime], InsertBatch],
        current_time: datetime,
    ) -> None:
        storage = get_writable_storage(storage_key)
        cluster = storage.get_cluster()
        clickhouse = cluster.get_query_connection(ClickhouseClientSettings.OPTIMIZE)
        table = storage.get_table_writer().get_schema().get_local_table_name()
        database = cluster.get_database()

        # no data, 0 partitions to optimize
        partitions = optimize.get_partitions_to_optimize(
            clickhouse, storage, database, table
        )
        assert partitions == []

        base = datetime(1999, 12, 26)  # a sunday
        base_monday = base - timedelta(days=base.weekday())

        # 1 event, 0 unoptimized partitions
        write_processed_messages(storage, [create_event_row_for_date(base)])
        partitions = optimize.get_partitions_to_optimize(
            clickhouse, storage, database, table
        )
        assert partitions == []

        # 2 events in the same part, 1 unoptimized part
        write_processed_messages(storage, [create_event_row_for_date(base)])
        partitions = optimize.get_partitions_to_optimize(
            clickhouse, storage, database, table
        )
        assert [(p.date, p.retention_days) for p in partitions] == [(base_monday, 90)]

        # 3 events in the same part, 1 unoptimized part
        write_processed_messages(storage, [create_event_row_for_date(base)])
        partitions = optimize.get_partitions_to_optimize(
            clickhouse, storage, database, table
        )
        assert [(p.date, p.retention_days) for p in partitions] == [(base_monday, 90)]

        # 3 events in one part, 2 in another, 2 unoptimized partitions
        a_month_earlier = base_monday - timedelta(days=31)
        a_month_earlier_monday = a_month_earlier - timedelta(
            days=a_month_earlier.weekday()
        )
        write_processed_messages(
            storage, [create_event_row_for_date(a_month_earlier_monday)]
        )
        write_processed_messages(
            storage, [create_event_row_for_date(a_month_earlier_monday)]
        )
        partitions = optimize.get_partitions_to_optimize(
            clickhouse, storage, database, table
        )
        assert [(p.date, p.retention_days) for p in partitions] == [
            (base_monday, 90),
            (a_month_earlier_monday, 90),
        ]

        # respects before (base is properly excluded)
        assert [
            (p.date, p.retention_days)
            for p in list(
                optimize.get_partitions_to_optimize(
                    clickhouse, storage, database, table, before=base
                )
            )
        ] == [(a_month_earlier_monday, 90)]

        scheduler = OptimizeScheduler(2)

        tracker = OptimizedPartitionTracker(
            redis_client=redis_client,
            host="some-hostname.domain.com",
            port=9000,
            database=database,
            table=table,
            expire_time=datetime.now() + timedelta(minutes=10),
        )

        tracker.update_all_partitions([part.name for part in partitions])
        with freeze_time(current_time):
            optimize.optimize_partition_runner(
                clickhouse=clickhouse,
                database=database,
                table=table,
                partitions=[part.name for part in partitions],
                scheduler=scheduler,
                tracker=tracker,
                clickhouse_host="some-hostname.domain.com",
            )

        # all partitions should be optimized
        partitions = optimize.get_partitions_to_optimize(
            clickhouse, storage, database, table
        )
        assert partitions == []

        tracker.delete_all_states()

    @pytest.mark.parametrize(
        "table,host,expected",
        [
            (
                "errors_local",
                None,
                {"table": "errors_local"},
            ),
            (
                "errors_local",
                "some-hostname.domain.com",
                {"table": "errors_local", "host": "some-hostname.domain.com"},
            ),
        ],
    )
    def test_metrics_tags(
        self, table: str, host: str, expected: Mapping[str, str]
    ) -> None:
        assert _get_metrics_tags(table, host) == expected


def test_optimize_partitions_raises_exception_with_cutoff_time() -> None:
    """
    Tests that a JobTimeoutException is raised when a cutoff time is reached.
    """
    storage = get_writable_storage(StorageKey.ERRORS)
    cluster = storage.get_cluster()
    clickhouse_pool = cluster.get_query_connection(ClickhouseClientSettings.OPTIMIZE)
    table = storage.get_table_writer().get_schema().get_local_table_name()
    database = cluster.get_database()

    tracker = OptimizedPartitionTracker(
        redis_client=redis_client,
        host="some-hostname.domain.com",
        port=9000,
        database=database,
        table=table,
        expire_time=datetime.now() + timedelta(minutes=10),
    )

    dummy_partition = "(90,'2022-03-28')"
    tracker.update_all_partitions([dummy_partition])
    scheduler = OptimizeScheduler(2)

    with freeze_time(
        last_midnight + settings.OPTIMIZE_JOB_CUTOFF_TIME + timedelta(minutes=15)
    ):
        with pytest.raises(OptimizedSchedulerTimeout):
            optimize_partition_runner(
                clickhouse=clickhouse_pool,
                database=database,
                table=table,
                partitions=[dummy_partition],
                scheduler=scheduler,
                tracker=tracker,
                clickhouse_host="some-hostname.domain.com",
            )

    tracker.delete_all_states()
