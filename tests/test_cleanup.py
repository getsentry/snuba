from base import BaseTest

from datetime import datetime, timedelta

from snuba import cleanup


class TestCleanup(BaseTest):
    def test(self):
        now = datetime(2000, 1, 1)

        parts = cleanup.get_active_partitions(self.clickhouse, self.database, self.table)
        assert parts == []

        # now, 90 retention
        self.write_processed_events(self.create_event_for_date(now))
        parts = cleanup.get_active_partitions(self.clickhouse, self.database, self.table)
        assert parts == [(now, 90)]
        stale = cleanup.filter_stale_partitions(parts, as_of=now)
        assert stale == []

        # -40 days, 90 retention
        forty_days_ago = now - timedelta(days=40)
        self.write_processed_events(self.create_event_for_date(forty_days_ago))
        parts = cleanup.get_active_partitions(self.clickhouse, self.database, self.table)
        assert parts == [(forty_days_ago, 90), (now, 90)]
        stale = cleanup.filter_stale_partitions(parts, as_of=now)
        assert stale == []

        # -100 days, 90 retention
        one_hundred_days_ago = now - timedelta(days=100)
        self.write_processed_events(self.create_event_for_date(one_hundred_days_ago))
        parts = cleanup.get_active_partitions(self.clickhouse, self.database, self.table)
        assert parts == [(one_hundred_days_ago, 90), (forty_days_ago, 90), (now, 90)]
        stale = cleanup.filter_stale_partitions(parts, as_of=now)
        assert stale == [(one_hundred_days_ago, 90)]

        # -1 day, 3 retention
        one_day_ago = now - timedelta(days=1)
        self.write_processed_events(self.create_event_for_date(one_day_ago, 3))
        parts = cleanup.get_active_partitions(self.clickhouse, self.database, self.table)
        assert parts == [(one_hundred_days_ago, 90), (forty_days_ago, 90), (one_day_ago, 3), (now, 90)]
        stale = cleanup.filter_stale_partitions(parts, as_of=now)
        assert stale == [(one_hundred_days_ago, 90)]

        # -5 days, 3 retention
        five_days_ago = now - timedelta(days=5)
        self.write_processed_events(self.create_event_for_date(five_days_ago, 3))
        parts = cleanup.get_active_partitions(self.clickhouse, self.database, self.table)
        assert parts == [(one_hundred_days_ago, 90), (forty_days_ago, 90),
                         (five_days_ago, 3), (one_day_ago, 3), (now, 90)]
        stale = cleanup.filter_stale_partitions(parts, as_of=now)
        assert stale == [(one_hundred_days_ago, 90), (five_days_ago, 3)]

        cleanup.drop_partitions(self.clickhouse, self.database, self.table, stale, dry_run=False)

        parts = cleanup.get_active_partitions(self.clickhouse, self.database, self.table)
        assert parts == [(forty_days_ago, 90), (one_day_ago, 3), (now, 90)]
