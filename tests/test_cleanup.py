from base import BaseTest

from datetime import datetime, timedelta
import mock
import time
import uuid

from snuba import cleanup, settings
from snuba.clickhouse import Clickhouse


class TestCleanup(BaseTest):
    def setup_method(self, test_method):
        super(TestCleanup, self).setup_method(test_method)

        self.clickhouse = Clickhouse('localhost')

    def test_blank(self):
        assert cleanup.get_active_partitions(self.clickhouse, self.database, self.table) == []

    @mock.patch('time.time')
    def test(self, time_mock):
        now = datetime(2000, 1, 1)
        time_mock.return_value = time.mktime(now.timetuple())

        def create_event(dt, retention_days=settings.DEFAULT_RETENTION_DAYS):
            event = {
                'event_id': uuid.uuid4().hex,
                'project_id': 1,
                'deleted': 0,
            }
            event['timestamp'] = time.mktime(dt.timetuple())
            event['retention_days'] = retention_days
            return event

        # now, 90 retention
        self.write_processed_events(create_event(now))
        parts = cleanup.get_active_partitions(self.clickhouse, self.database, self.table)
        assert parts == [(now, 90)]
        stale = cleanup.filter_stale_partitions(parts)
        assert stale == []

        # -40 days, 90 retention
        forty_days_ago = now - timedelta(days=40)
        self.write_processed_events(create_event(forty_days_ago))
        parts = cleanup.get_active_partitions(self.clickhouse, self.database, self.table)
        assert parts == [(forty_days_ago, 90), (now, 90)]
        stale = cleanup.filter_stale_partitions(parts)
        assert stale == []

        # -100 days, 90 retention
        one_hundred_days_ago = now - timedelta(days=100)
        self.write_processed_events(create_event(one_hundred_days_ago))
        parts = cleanup.get_active_partitions(self.clickhouse, self.database, self.table)
        assert parts == [(one_hundred_days_ago, 90), (forty_days_ago, 90), (now, 90)]
        stale = cleanup.filter_stale_partitions(parts)
        assert stale == [(one_hundred_days_ago, 90)]

        # -1 day, 3 retention
        one_day_ago = now - timedelta(days=1)
        self.write_processed_events(create_event(one_day_ago, 3))
        parts = cleanup.get_active_partitions(self.clickhouse, self.database, self.table)
        assert parts == [(one_hundred_days_ago, 90), (forty_days_ago, 90), (one_day_ago, 3), (now, 90)]
        stale = cleanup.filter_stale_partitions(parts)
        assert stale == [(one_hundred_days_ago, 90)]

        # -5 days, 3 retention
        five_days_ago = now - timedelta(days=5)
        self.write_processed_events(create_event(five_days_ago, 3))
        parts = cleanup.get_active_partitions(self.clickhouse, self.database, self.table)
        assert parts == [(one_hundred_days_ago, 90), (forty_days_ago, 90),
                         (five_days_ago, 3), (one_day_ago, 3), (now, 90)]
        stale = cleanup.filter_stale_partitions(parts)
        assert stale == [(one_hundred_days_ago, 90), (five_days_ago, 3)]

        cleanup.drop_partitions(self.clickhouse, self.database, self.table, stale, dry_run=False)

        parts = cleanup.get_active_partitions(self.clickhouse, self.database, self.table)
        assert parts == [(forty_days_ago, 90), (one_day_ago, 3), (now, 90)]