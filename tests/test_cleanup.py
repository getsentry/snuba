import uuid
from datetime import datetime, timedelta
from typing import Callable, Optional

import pytest

from snuba import cleanup, settings
from snuba.clusters.cluster import ClickhouseClientSettings
from snuba.datasets.storages import StorageKey
from snuba.datasets.storages.factory import get_writable_storage
from snuba.processor import InsertBatch, JSONRowInsertBatch, json_encode_insert_batch
from tests.helpers import write_processed_messages

test_data = [
    pytest.param(
        StorageKey.EVENTS,
        lambda dt, retention: InsertBatch(
            [
                {
                    "event_id": uuid.uuid4().hex,
                    "project_id": 1,
                    "group_id": 1,
                    "deleted": 0,
                    "timestamp": dt,
                    "retention_days": retention or settings.DEFAULT_RETENTION_DAYS,
                }
            ],
            None,
        ),
        id="events",
    ),
    pytest.param(
        StorageKey.ERRORS,
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
        StorageKey.TRANSACTIONS,
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


class TestCleanup:
    @pytest.mark.parametrize(
        "storage_key, create_event_row_for_date", test_data,
    )
    def test(
        self,
        storage_key: StorageKey,
        create_event_row_for_date: Callable[[datetime, Optional[int]], InsertBatch],
    ) -> None:
        def create_event(
            date: datetime, retention: Optional[int]
        ) -> JSONRowInsertBatch:
            return json_encode_insert_batch(create_event_row_for_date(date, retention))

        def to_monday(d: datetime) -> datetime:
            return d - timedelta(days=d.weekday())

        base = datetime(1999, 12, 26)  # a sunday

        storage = get_writable_storage(storage_key)
        clickhouse = storage.get_cluster().get_query_connection(
            ClickhouseClientSettings.CLEANUP
        )

        table = storage.get_table_writer().get_schema().get_table_name()
        database = storage.get_cluster().get_database()

        parts = cleanup.get_active_partitions(clickhouse, storage, database, table)
        assert parts == []

        # base, 90 retention
        write_processed_messages(storage, [create_event(base, None)])
        parts = cleanup.get_active_partitions(clickhouse, storage, database, table)

        assert [(p.date, p.retention_days) for p in parts] == [(to_monday(base), 90)]
        stale = cleanup.filter_stale_partitions(parts, as_of=base)
        assert stale == []

        # -40 days, 90 retention
        three_weeks_ago = base - timedelta(days=7 * 3)
        write_processed_messages(storage, [create_event(three_weeks_ago, None)])
        parts = cleanup.get_active_partitions(clickhouse, storage, database, table)
        assert [(p.date, p.retention_days) for p in parts] == [
            (to_monday(three_weeks_ago), 90),
            (to_monday(base), 90),
        ]
        stale = cleanup.filter_stale_partitions(parts, as_of=base)
        assert stale == []

        # -100 days, 90 retention
        thirteen_weeks_ago = base - timedelta(days=7 * 13)
        write_processed_messages(storage, [create_event(thirteen_weeks_ago, None)])
        parts = cleanup.get_active_partitions(clickhouse, storage, database, table)
        assert [(p.date, p.retention_days) for p in parts] == [
            (to_monday(thirteen_weeks_ago), 90),
            (to_monday(three_weeks_ago), 90),
            (to_monday(base), 90),
        ]
        stale = cleanup.filter_stale_partitions(parts, as_of=base)
        assert [(p.date, p.retention_days) for p in stale] == [
            (to_monday(thirteen_weeks_ago), 90)
        ]

        # -1 week, 30 retention
        one_week_ago = base - timedelta(days=7)
        write_processed_messages(storage, [create_event(one_week_ago, 30)])
        parts = cleanup.get_active_partitions(clickhouse, storage, database, table)

        assert {(p.date, p.retention_days) for p in parts} == set(
            [
                (to_monday(thirteen_weeks_ago), 90),
                (to_monday(three_weeks_ago), 90),
                (to_monday(one_week_ago), 30),
                (to_monday(base), 90),
            ]
        )
        stale = cleanup.filter_stale_partitions(parts, as_of=base)
        assert [(p.date, p.retention_days) for p in stale] == [
            (to_monday(thirteen_weeks_ago), 90)
        ]

        # -5 weeks, 30 retention
        five_weeks_ago = base - timedelta(days=7 * 5)
        write_processed_messages(storage, [create_event(five_weeks_ago, 30)])
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
        stale = cleanup.filter_stale_partitions(parts, as_of=base)
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
