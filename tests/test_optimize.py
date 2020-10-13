import uuid
from datetime import datetime, timedelta
from typing import Any, Mapping

from snuba import optimize, settings
from snuba.clusters.cluster import ClickhouseClientSettings
from snuba.datasets.storages import StorageKey
from snuba.datasets.storages.factory import get_writable_storage
from snuba.processor import InsertBatch
from tests.helpers import write_processed_messages


class TestOptimize:
    def test(self) -> None:
        storage = get_writable_storage(StorageKey.EVENTS)
        cluster = storage.get_cluster()
        clickhouse = cluster.get_query_connection(ClickhouseClientSettings.OPTIMIZE)
        table = storage.get_table_writer().get_schema().get_table_name()
        database = cluster.get_database()

        # no data, 0 partitions to optimize
        parts = optimize.get_partitions_to_optimize(clickhouse, database, table)
        assert parts == []

        base = datetime(1999, 12, 26)  # a sunday
        base_monday = base - timedelta(days=base.weekday())

        # 1 event, 0 unoptimized parts
        write_processed_messages(storage, [self.create_event_row_for_date(base)])
        parts = optimize.get_partitions_to_optimize(clickhouse, database, table)
        assert parts == []

        # 2 events in the same part, 1 unoptimized part
        write_processed_messages(storage, [self.create_event_row_for_date(base)])
        parts = optimize.get_partitions_to_optimize(clickhouse, database, table)
        assert parts == [(base_monday, 90)]

        # 3 events in the same part, 1 unoptimized part
        write_processed_messages(storage, [self.create_event_row_for_date(base)])
        parts = optimize.get_partitions_to_optimize(clickhouse, database, table)
        assert parts == [(base_monday, 90)]

        # 3 events in one part, 2 in another, 2 unoptimized parts
        a_month_earlier = base_monday - timedelta(days=31)
        a_month_earlier_monday = a_month_earlier - timedelta(
            days=a_month_earlier.weekday()
        )
        write_processed_messages(
            storage, [self.create_event_row_for_date(a_month_earlier_monday)]
        )
        write_processed_messages(
            storage, [self.create_event_row_for_date(a_month_earlier_monday)]
        )
        parts = optimize.get_partitions_to_optimize(clickhouse, database, table)
        assert parts == [(base_monday, 90), (a_month_earlier_monday, 90)]

        # respects before (base is properly excluded)
        assert list(
            optimize.get_partitions_to_optimize(
                clickhouse, database, table, before=base
            )
        ) == [(a_month_earlier_monday, 90)]

        optimize.optimize_partitions(clickhouse, database, table, parts)

        # all parts should be optimized
        parts = optimize.get_partitions_to_optimize(clickhouse, database, table)
        assert parts == []

    def create_event_row_for_date(self, dt: datetime) -> Mapping[str, Any]:
        return InsertBatch(
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
        )
