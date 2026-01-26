import uuid
from datetime import UTC, datetime, timedelta
from typing import Callable, Mapping
from unittest.mock import Mock, patch

import pytest
import time_machine

from snuba import settings
from snuba.clickhouse.errors import ClickhouseError
from snuba.clickhouse.optimize import optimize
from snuba.clickhouse.optimize.optimize import (
    _get_metrics_tags,
    optimize_partition_runner,
    should_optimize_partition_today,
)
from snuba.clickhouse.optimize.optimize_scheduler import OptimizedSchedulerTimeout
from snuba.clickhouse.optimize.optimize_tracker import OptimizedPartitionTracker
from snuba.clusters.cluster import ClickhouseClientSettings
from snuba.datasets.storages.factory import get_writable_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.processor import InsertBatch
from snuba.redis import RedisClientKey, get_redis_client
from tests.helpers import write_processed_messages

redis_client = get_redis_client(RedisClientKey.REPLACEMENTS_STORE)

last_midnight = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0).astimezone(UTC)

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
        + timedelta(hours=settings.PARALLEL_OPTIMIZE_JOB_START_TIME)
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
        + timedelta(hours=settings.PARALLEL_OPTIMIZE_JOB_START_TIME)
        + timedelta(minutes=30),
        id="transactions parallel",
    ),
]


@pytest.mark.xfail(
    reason="This test still is flaky sometimes and then completely blocks CI / deployment"
)
class TestOptimize:
    @pytest.mark.clickhouse_db
    @pytest.mark.redis_db
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
        partitions = optimize.get_partitions_from_clickhouse(clickhouse, storage, database, table)
        assert partitions == []

        base = datetime(1999, 12, 26)  # a sunday
        base_monday = base - timedelta(days=base.weekday())

        # 1 event, 0 unoptimized partitions
        write_processed_messages(storage, [create_event_row_for_date(base)])
        partitions = optimize.get_partitions_from_clickhouse(clickhouse, storage, database, table)
        assert partitions == []

        # 2 events in the same part, 1 unoptimized part
        write_processed_messages(storage, [create_event_row_for_date(base)])
        partitions = optimize.get_partitions_from_clickhouse(clickhouse, storage, database, table)
        assert [(p.date, p.retention_days) for p in partitions] == [(base_monday, 90)]

        # 3 events in the same part, 1 unoptimized part
        write_processed_messages(storage, [create_event_row_for_date(base)])
        partitions = optimize.get_partitions_from_clickhouse(clickhouse, storage, database, table)
        assert [(p.date, p.retention_days) for p in partitions] == [(base_monday, 90)]

        # 3 events in one part, 2 in another, 2 unoptimized partitions
        a_month_earlier = base_monday - timedelta(days=31)
        a_month_earlier_monday = a_month_earlier - timedelta(days=a_month_earlier.weekday())
        write_processed_messages(storage, [create_event_row_for_date(a_month_earlier_monday)])
        write_processed_messages(storage, [create_event_row_for_date(a_month_earlier_monday)])
        partitions = optimize.get_partitions_from_clickhouse(clickhouse, storage, database, table)
        assert sorted([(p.date, p.retention_days) for p in partitions]) == sorted(
            [
                (base_monday, 90),
                (a_month_earlier_monday, 90),
            ]
        )

        # respects before (base is properly excluded)
        assert [
            (p.date, p.retention_days)
            for p in list(
                optimize.get_partitions_from_clickhouse(
                    clickhouse, storage, database, table, before=base
                )
            )
        ] == [(a_month_earlier_monday, 90)]

        tracker = OptimizedPartitionTracker(
            redis_client=redis_client,
            host="some-hostname.domain.com",
            port=9000,
            database=database,
            table=table,
            expire_time=datetime.now() + timedelta(minutes=10),
        )

        tracker.update_all_partitions([part.name for part in partitions])
        with time_machine.travel(current_time, tick=False):
            optimize.optimize_partition_runner(
                clickhouse=clickhouse,
                database=database,
                table=table,
                partitions=[part.name for part in partitions],
                default_parallel_threads=2,
                tracker=tracker,
                clickhouse_host="some-hostname.domain.com",
            )

        # all partitions should be optimized
        partitions = optimize.get_partitions_from_clickhouse(clickhouse, storage, database, table)
        assert partitions == []

        tracker.delete_all_states()
        # For ClickHouse 23.3 and 23.8 parts from previous test runs
        # interfere with following tests, so best to drop the tables
        clickhouse.execute(f"DROP TABLE IF EXISTS {database}.{table} SYNC")

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
    def test_metrics_tags(self, table: str, host: str, expected: Mapping[str, str]) -> None:
        assert _get_metrics_tags(table, host) == expected


test_data = [
    pytest.param(
        StorageKey.ERRORS,
        last_midnight
        + timedelta(hours=settings.PARALLEL_OPTIMIZE_JOB_START_TIME)
        + timedelta(minutes=30),
        id="errors parallel",
    )
]


class TestOptimizeError:
    @pytest.mark.clickhouse_db
    @pytest.mark.redis_db
    @pytest.mark.parametrize(
        "storage_key, current_time",
        test_data,
    )
    def test_optimize_partition_runner_errors(
        self,
        storage_key: StorageKey,
        current_time: datetime,
    ) -> None:
        storage = get_writable_storage(storage_key)
        cluster = storage.get_cluster()
        clickhouse = cluster.get_query_connection(ClickhouseClientSettings.OPTIMIZE)
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

        with time_machine.travel(current_time, tick=False):
            with patch(
                "snuba.clickhouse.optimize.optimize.optimize_partitions",
                side_effect=ClickhouseError(),
            ):
                with pytest.raises(ClickhouseError):
                    optimize.optimize_partition_runner(
                        clickhouse=clickhouse,
                        database=database,
                        table=table,
                        partitions=["1", "2", "3", "4", "5", "6", "7", "8", "9", "10"],
                        default_parallel_threads=3,
                        tracker=tracker,
                        clickhouse_host="some-hostname.domain.com",
                    )

        # For ClickHouse 23.3 and 23.8 parts from previous test runs
        # interfere with following tests, so best to drop the tables
        clickhouse.execute(f"DROP TABLE IF EXISTS {database}.{table} SYNC")


class TestOptimizeFrequency:
    @pytest.mark.clickhouse_db
    @pytest.mark.redis_db
    @pytest.mark.parametrize(
        "storage_key, current_time",
        test_data,
    )
    @patch("snuba.clickhouse.optimize.optimize.optimize_partitions")
    def test_optimize_partitions_is_called_the_same_number_of_times_as_number_of_partitions(
        self,
        mock_optimize_partitions: Mock,
        storage_key: StorageKey,
        current_time: datetime,
    ) -> None:
        storage = get_writable_storage(storage_key)
        cluster = storage.get_cluster()
        clickhouse = cluster.get_query_connection(ClickhouseClientSettings.OPTIMIZE)
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

        with time_machine.travel(current_time, tick=False):
            optimize.optimize_partition_runner(
                clickhouse=clickhouse,
                database=database,
                table=table,
                partitions=["1", "2", "3", "4", "5", "6", "7", "8", "9", "10"],
                default_parallel_threads=3,
                tracker=tracker,
                clickhouse_host="some-hostname.domain.com",
            )
        assert mock_optimize_partitions.call_count == 10

        # For ClickHouse 23.3 and 23.8 parts from previous test runs
        # interfere with following tests, so best to drop the tables
        clickhouse.execute(f"DROP TABLE IF EXISTS {database}.{table} SYNC")


@pytest.mark.clickhouse_db
def test_optimize_partitions_raises_exception_with_cutoff_time() -> None:
    """
    Tests that a JobTimeoutException is raised when a cutoff time is reached.
    """
    prev_job_cutoff_time = settings.OPTIMIZE_JOB_CUTOFF_TIME
    settings.OPTIMIZE_JOB_CUTOFF_TIME = 23
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

    with time_machine.travel(
        last_midnight + timedelta(hours=settings.OPTIMIZE_JOB_CUTOFF_TIME) + timedelta(minutes=15),
        tick=False,
    ):
        with pytest.raises(OptimizedSchedulerTimeout):
            optimize_partition_runner(
                clickhouse=clickhouse_pool,
                database=database,
                table=table,
                partitions=[dummy_partition],
                default_parallel_threads=2,
                tracker=tracker,
                clickhouse_host="some-hostname.domain.com",
            )

    tracker.delete_all_states()
    settings.OPTIMIZE_JOB_CUTOFF_TIME = prev_job_cutoff_time


def test_should_optimize_partition_today_two_divisions() -> None:
    # There's no semantic importance to which partitions should
    # be optimized, but we want to make sure some land in group 0 and
    # some land in group 1, and that the results are consistent so that
    # we know we can trust that each group gets optimized once every 2 days
    #
    # Keep the "time" constant so that we get consistent results
    day_0_partitions = ["2025-01-01", "2025-01-03", "2025-01-04"]
    day_1_partitions = ["2025-01-02", "2025-01-05"]

    with time_machine.travel(datetime(2025, 1, 13).astimezone(UTC), tick=False):
        for day in day_0_partitions:
            assert should_optimize_partition_today(day, 2)
        for day in day_1_partitions:
            assert not should_optimize_partition_today(day, 2)

    # Inverse of above
    with time_machine.travel(datetime(2025, 1, 14).astimezone(UTC), tick=False):
        for day in day_1_partitions:
            assert should_optimize_partition_today(day, 2)
        for day in day_0_partitions:
            assert not should_optimize_partition_today(day, 2)


def test_should_optimize_partition_today_one_division() -> None:
    # Tests that all partitions are optimized if there is only one division
    assert should_optimize_partition_today("2025-01-01", 1)
    assert should_optimize_partition_today("2025-01-02", 1)
    assert should_optimize_partition_today("2025-01-03", 1)
    assert should_optimize_partition_today("2025-01-04", 1)
