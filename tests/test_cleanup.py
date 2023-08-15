import uuid
from datetime import datetime, timedelta
from typing import Callable, Optional
from unittest.mock import MagicMock, patch

import pytest

from snuba import cleanup, settings
from snuba.clusters.cluster import ClickhouseClientSettings
from snuba.datasets.storages.factory import get_writable_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.processor import InsertBatch
from tests.helpers import write_processed_messages

test_data = [
    pytest.param(
        StorageKey("errors"),
        lambda dt, retention: InsertBatch(
            [
                {
                    "event_id": str(uuid.uuid4()),
                    "project_id": 1,
                    "group_id": 1,
                    "deleted": 0,
                    "timestamp": dt,
                    "retention_days": retention or settings.DEFAULT_RETENTION_DAYS,
                }
            ],
            None,
        ),
        id="errors",
    ),
    pytest.param(
        StorageKey("transactions"),
        lambda dt, retention: InsertBatch(
            [
                {
                    "event_id": str(uuid.uuid4()),
                    "project_id": 1,
                    "deleted": 0,
                    "finish_ts": dt,
                    "retention_days": retention or settings.DEFAULT_RETENTION_DAYS,
                }
            ],
            None,
        ),
        id="transactions",
    ),
]


@pytest.mark.clickhouse_db
@pytest.mark.redis_db
class TestCleanup:
    @pytest.mark.parametrize(
        "storage_key, create_event_row_for_date",
        test_data,
    )
    @patch("snuba.cleanup.current_time")
    def test_main_cases(
        self,
        current_time: MagicMock,
        storage_key: StorageKey,
        create_event_row_for_date: Callable[[datetime, Optional[int]], InsertBatch],
    ) -> None:
        def to_monday(d: datetime) -> datetime:
            rounded = d - timedelta(days=d.weekday())
            return datetime(rounded.year, rounded.month, rounded.day)

        # In prod the dates have hours/seconds etc.
        # use future dates to avoid hitting TTLs
        base = datetime(2100, 1, 3, 1, 14, 35)  # a sunday

        current_time.return_value = base

        storage = get_writable_storage(storage_key)
        clickhouse = storage.get_cluster().get_query_connection(
            ClickhouseClientSettings.CLEANUP
        )

        table = storage.get_table_writer().get_schema().get_local_table_name()

        database = storage.get_cluster().get_database()

        parts = cleanup.get_active_partitions(clickhouse, storage, database, table)
        assert parts == []

        # base, 90 retention
        write_processed_messages(storage, [create_event_row_for_date(base, None)])
        parts = cleanup.get_active_partitions(clickhouse, storage, database, table)

        assert [(p.date, p.retention_days) for p in parts] == [(to_monday(base), 90)]
        stale = cleanup.filter_stale_partitions(parts)
        assert stale == []

        # -40 days, 90 retention
        three_weeks_ago = base - timedelta(days=7 * 3)
        write_processed_messages(
            storage, [create_event_row_for_date(three_weeks_ago, None)]
        )
        parts = cleanup.get_active_partitions(clickhouse, storage, database, table)
        assert [(p.date, p.retention_days) for p in parts] == [
            (to_monday(three_weeks_ago), 90),
            (to_monday(base), 90),
        ]
        stale = cleanup.filter_stale_partitions(parts)
        assert stale == []

        # -100 days, 90 retention
        thirteen_weeks_ago = base - timedelta(days=7 * 13)
        write_processed_messages(
            storage, [create_event_row_for_date(thirteen_weeks_ago, None)]
        )
        parts = cleanup.get_active_partitions(clickhouse, storage, database, table)
        assert [(p.date, p.retention_days) for p in parts] == [
            (to_monday(thirteen_weeks_ago), 90),
            (to_monday(three_weeks_ago), 90),
            (to_monday(base), 90),
        ]
        stale = cleanup.filter_stale_partitions(parts)
        assert [(p.date, p.retention_days) for p in stale] == [
            (to_monday(thirteen_weeks_ago), 90)
        ]

        # -1 week, 30 retention
        one_week_ago = base - timedelta(days=7)
        write_processed_messages(storage, [create_event_row_for_date(one_week_ago, 30)])
        parts = cleanup.get_active_partitions(clickhouse, storage, database, table)

        assert {(p.date, p.retention_days) for p in parts} == set(
            [
                (to_monday(thirteen_weeks_ago), 90),
                (to_monday(three_weeks_ago), 90),
                (to_monday(one_week_ago), 30),
                (to_monday(base), 90),
            ]
        )
        stale = cleanup.filter_stale_partitions(parts)
        assert [(p.date, p.retention_days) for p in stale] == [
            (to_monday(thirteen_weeks_ago), 90)
        ]

        # -5 weeks, 30 retention
        five_weeks_ago = base - timedelta(days=7 * 5)
        write_processed_messages(
            storage, [create_event_row_for_date(five_weeks_ago, 30)]
        )
        parts = cleanup.get_active_partitions(clickhouse, storage, database, table)
        assert {(p.date, p.retention_days) for p in parts} == set(
            [
                (to_monday(thirteen_weeks_ago), 90),
                (to_monday(five_weeks_ago), 30),
                (to_monday(three_weeks_ago), 90),
                (to_monday(one_week_ago), 30),
                (to_monday(base), 90),
            ]
        )
        stale = cleanup.filter_stale_partitions(parts)
        assert {(p.date, p.retention_days) for p in stale} == set(
            [(to_monday(thirteen_weeks_ago), 90), (to_monday(five_weeks_ago), 30)]
        )

        cleanup.drop_partitions(clickhouse, database, table, stale, dry_run=False)

        parts = cleanup.get_active_partitions(clickhouse, storage, database, table)
        assert {(p.date, p.retention_days) for p in parts} == set(
            [
                (to_monday(three_weeks_ago), 90),
                (to_monday(one_week_ago), 30),
                (to_monday(base), 90),
            ]
        )

    @pytest.mark.parametrize(
        "storage_key, create_event_row_for_date",
        test_data,
    )
    @patch("snuba.cleanup.current_time")
    def test_midnight_error_case(
        self,
        current_time: MagicMock,
        storage_key: StorageKey,
        create_event_row_for_date: Callable[[datetime, Optional[int]], InsertBatch],
    ) -> None:
        """
        This test is simulating a failure case that happened in production, where when the script ran,
        it attempted to delete a part whose last day was within the retention period. The script was
        using datetimes not aligned to midnight, and so it was removing a part on the same day, but
        technically outside of the window because of the script's extra time.

        """

        def to_monday(d: datetime) -> datetime:
            rounded = d - timedelta(days=d.weekday())
            return datetime(rounded.year, rounded.month, rounded.day)

        storage = get_writable_storage(storage_key)
        clickhouse = storage.get_cluster().get_query_connection(
            ClickhouseClientSettings.CLEANUP
        )

        table = storage.get_table_writer().get_schema().get_local_table_name()
        database = storage.get_cluster().get_database()

        parts = cleanup.get_active_partitions(clickhouse, storage, database, table)
        assert parts == []

        # Pick a time a few minutes after midnight
        base = datetime(2022, 1, 29, 0, 4, 37)
        current_time.return_value = base

        # Insert an event that is outside retention, but its last day is just inside retention
        # Note that without rounding the base time to midnight, base - retention > last_day(timestamp)
        timestamp = datetime(2021, 10, 25)
        write_processed_messages(storage, [create_event_row_for_date(timestamp, 90)])
        parts = cleanup.get_active_partitions(clickhouse, storage, database, table)

        assert [(p.date, p.retention_days) for p in parts] == [
            (to_monday(timestamp), 90)
        ]
        stale = cleanup.filter_stale_partitions(parts)
        assert stale == []
