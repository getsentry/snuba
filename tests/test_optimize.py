import uuid
from datetime import datetime, timedelta
from typing import Callable

import pytest

from snuba import optimize, settings
from snuba.clusters.cluster import ClickhouseClientSettings
from snuba.datasets.storages import StorageKey
from snuba.datasets.storages.factory import get_writable_storage
from snuba.processor import InsertBatch
from tests.helpers import write_processed_messages


test_data = [
    pytest.param(
        StorageKey.EVENTS,
        lambda dt: InsertBatch(
            [
                {
                    "event_id": uuid.uuid4().hex,
                    "project_id": 1,
                    "group_id": 1,
                    "deleted": 0,
                    "timestamp": dt,
                    "retention_days": settings.DEFAULT_RETENTION_DAYS,
                }
            ]
        ),
        id="events",
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
            ]
        ),
        id="errors",
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
            ]
        ),
        id="transactions",
    ),
]


class TestOptimize:
    @pytest.mark.parametrize(
        "storage_key, create_event_row_for_date", test_data,
    )
    def test_optimize(
        self,
        storage_key: StorageKey,
        create_event_row_for_date: Callable[[datetime], InsertBatch],
    ) -> None:
        storage = get_writable_storage(storage_key)
        cluster = storage.get_cluster()
        clickhouse = cluster.get_query_connection(ClickhouseClientSettings.OPTIMIZE)
        table = storage.get_table_writer().get_schema().get_table_name()
        database = cluster.get_database()

        # no data, 0 partitions to optimize
        parts = optimize.get_partitions_to_optimize(
            clickhouse, storage_key, database, table
        )
        assert parts == []

        base = datetime(1999, 12, 26)  # a sunday
        base_monday = base - timedelta(days=base.weekday())

        # 1 event, 0 unoptimized parts
        write_processed_messages(storage, [create_event_row_for_date(base)])
        parts = optimize.get_partitions_to_optimize(
            clickhouse, storage_key, database, table
        )
        assert parts == []

        # 2 events in the same part, 1 unoptimized part
        write_processed_messages(storage, [create_event_row_for_date(base)])
        parts = optimize.get_partitions_to_optimize(
            clickhouse, storage_key, database, table
        )
        assert parts == [(base_monday, 90)]

        # 3 events in the same part, 1 unoptimized part
        write_processed_messages(storage, [create_event_row_for_date(base)])
        parts = optimize.get_partitions_to_optimize(
            clickhouse, storage_key, database, table
        )
        assert parts == [(base_monday, 90)]

        # 3 events in one part, 2 in another, 2 unoptimized parts
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
        parts = optimize.get_partitions_to_optimize(
            clickhouse, storage_key, database, table
        )
        assert parts == [(base_monday, 90), (a_month_earlier_monday, 90)]

        # respects before (base is properly excluded)
        assert list(
            optimize.get_partitions_to_optimize(
                clickhouse, storage_key, database, table, before=base
            )
        ) == [(a_month_earlier_monday, 90)]

        optimize.optimize_partitions(clickhouse, storage_key, database, table, parts)

        # all parts should be optimized
        parts = optimize.get_partitions_to_optimize(
            clickhouse, storage_key, database, table
        )
        assert parts == []
