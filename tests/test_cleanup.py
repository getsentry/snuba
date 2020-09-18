from tests.base import BaseEventsTest

from datetime import datetime, timedelta

from snuba import cleanup
from snuba.clusters.cluster import ClickhouseClientSettings
from snuba.datasets.storages import StorageKey
from snuba.datasets.storages.factory import get_storage


class TestCleanup(BaseEventsTest):
    def test(self) -> None:
        def to_monday(d):
            return d - timedelta(days=d.weekday())

        base = datetime(1999, 12, 26)  # a sunday

        cluster = get_storage(StorageKey.EVENTS).get_cluster()
        database = cluster.get_database()

        clickhouse = cluster.get_query_connection(ClickhouseClientSettings.CLEANUP)

        parts = cleanup.get_active_partitions(clickhouse, database, self.table)
        assert parts == []

        # base, 90 retention
        self.write_rows([self.create_event_row_for_date(base)])
        parts = cleanup.get_active_partitions(clickhouse, database, self.table)
        assert parts == [(to_monday(base), 90)]
        stale = cleanup.filter_stale_partitions(parts, as_of=base)
        assert stale == []

        # -40 days, 90 retention
        three_weeks_ago = base - timedelta(days=7 * 3)
        self.write_rows([self.create_event_row_for_date(three_weeks_ago)])
        parts = cleanup.get_active_partitions(clickhouse, database, self.table)
        assert parts == [(to_monday(three_weeks_ago), 90), (to_monday(base), 90)]
        stale = cleanup.filter_stale_partitions(parts, as_of=base)
        assert stale == []

        # -100 days, 90 retention
        thirteen_weeks_ago = base - timedelta(days=7 * 13)
        self.write_rows([self.create_event_row_for_date(thirteen_weeks_ago)])
        parts = cleanup.get_active_partitions(clickhouse, database, self.table)
        assert parts == [
            (to_monday(thirteen_weeks_ago), 90),
            (to_monday(three_weeks_ago), 90),
            (to_monday(base), 90),
        ]
        stale = cleanup.filter_stale_partitions(parts, as_of=base)
        assert stale == [(to_monday(thirteen_weeks_ago), 90)]

        # -1 week, 30 retention
        one_week_ago = base - timedelta(days=7)
        self.write_rows([self.create_event_row_for_date(one_week_ago, 30)])
        parts = cleanup.get_active_partitions(clickhouse, database, self.table)
        assert parts == [
            (to_monday(thirteen_weeks_ago), 90),
            (to_monday(three_weeks_ago), 90),
            (to_monday(one_week_ago), 30),
            (to_monday(base), 90),
        ]
        stale = cleanup.filter_stale_partitions(parts, as_of=base)
        assert stale == [(to_monday(thirteen_weeks_ago), 90)]

        # -5 weeks, 30 retention
        five_weeks_ago = base - timedelta(days=7 * 5)
        self.write_rows([self.create_event_row_for_date(five_weeks_ago, 30)])
        parts = cleanup.get_active_partitions(clickhouse, database, self.table)
        assert parts == [
            (to_monday(thirteen_weeks_ago), 90),
            (to_monday(five_weeks_ago), 30),
            (to_monday(three_weeks_ago), 90),
            (to_monday(one_week_ago), 30),
            (to_monday(base), 90),
        ]
        stale = cleanup.filter_stale_partitions(parts, as_of=base)
        assert stale == [
            (to_monday(thirteen_weeks_ago), 90),
            (to_monday(five_weeks_ago), 30),
        ]

        cleanup.drop_partitions(clickhouse, database, self.table, stale, dry_run=False)

        parts = cleanup.get_active_partitions(clickhouse, database, self.table)
        assert parts == [
            (to_monday(three_weeks_ago), 90),
            (to_monday(one_week_ago), 30),
            (to_monday(base), 90),
        ]
