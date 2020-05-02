from tests.base import BaseEventsTest

from datetime import datetime, timedelta

from snuba import optimize
from snuba.clusters.cluster import ClickhouseClientSettings
from snuba.datasets.storages import StorageKey
from snuba.datasets.storages.factory import get_storage


class TestOptimize(BaseEventsTest):
    def test(self):
        clickhouse = (
            get_storage(StorageKey.EVENTS)
            .get_cluster()
            .get_connection(ClickhouseClientSettings.OPTIMIZE)
        )
        # no data, 0 partitions to optimize
        parts = optimize.get_partitions_to_optimize(
            clickhouse, self.database, self.table
        )
        assert parts == []

        base = datetime(1999, 12, 26)  # a sunday
        base_monday = base - timedelta(days=base.weekday())

        # 1 event, 0 unoptimized parts
        self.write_processed_records(self.create_event_for_date(base))
        parts = optimize.get_partitions_to_optimize(
            clickhouse, self.database, self.table
        )
        assert parts == []

        # 2 events in the same part, 1 unoptimized part
        self.write_processed_records(self.create_event_for_date(base))
        parts = optimize.get_partitions_to_optimize(
            clickhouse, self.database, self.table
        )
        assert parts == [(base_monday, 90)]

        # 3 events in the same part, 1 unoptimized part
        self.write_processed_records(self.create_event_for_date(base))
        parts = optimize.get_partitions_to_optimize(
            clickhouse, self.database, self.table
        )
        assert parts == [(base_monday, 90)]

        # 3 events in one part, 2 in another, 2 unoptimized parts
        a_month_earlier = base_monday - timedelta(days=31)
        a_month_earlier_monday = a_month_earlier - timedelta(
            days=a_month_earlier.weekday()
        )
        self.write_processed_records(self.create_event_for_date(a_month_earlier_monday))
        self.write_processed_records(self.create_event_for_date(a_month_earlier_monday))
        parts = optimize.get_partitions_to_optimize(
            clickhouse, self.database, self.table
        )
        assert parts == [(base_monday, 90), (a_month_earlier_monday, 90)]

        # respects before (base is properly excluded)
        assert list(
            optimize.get_partitions_to_optimize(
                clickhouse, self.database, self.table, before=base
            )
        ) == [(a_month_earlier_monday, 90)]

        optimize.optimize_partitions(clickhouse, self.database, self.table, parts)

        # all parts should be optimized
        parts = optimize.get_partitions_to_optimize(
            clickhouse, self.database, self.table
        )
        assert parts == []
