from base import BaseTest

from datetime import datetime, timedelta
import time
import uuid

from snuba import optimize, settings
from snuba.clickhouse import Clickhouse


class TestOptimize(BaseTest):
    def test(self):
        # no data, 0 partitions to optimize
        parts = optimize.get_partitions_to_optimize(self.clickhouse, self.database, self.table)
        assert parts == []

        now = datetime(2000, 1, 1)

        def create_event(dt, retention_days=settings.DEFAULT_RETENTION_DAYS):
            event = {
                'event_id': uuid.uuid4().hex,
                'project_id': 1,
                'deleted': 0,
            }
            event['timestamp'] = time.mktime(dt.timetuple())
            event['retention_days'] = retention_days
            return event

        # 1 event, 0 unoptimized parts
        self.write_processed_events(create_event(now))
        parts = optimize.get_partitions_to_optimize(self.clickhouse, self.database, self.table)
        assert parts == []

        # 2 events in the same part, 1 unoptimized part
        self.write_processed_events(create_event(now))
        parts = optimize.get_partitions_to_optimize(self.clickhouse, self.database, self.table)
        assert parts == [(now, 90)]

        # 3 events in the same part, 1 unoptimized part
        self.write_processed_events(create_event(now))
        parts = optimize.get_partitions_to_optimize(self.clickhouse, self.database, self.table)
        assert parts == [(now, 90)]

        # 3 events in one part, 2 in another, 2 unoptimized parts
        a_month_earlier = now - timedelta(days=31)
        self.write_processed_events(create_event(a_month_earlier))
        self.write_processed_events(create_event(a_month_earlier))
        parts = optimize.get_partitions_to_optimize(self.clickhouse, self.database, self.table)
        assert parts == [(now, 90), (a_month_earlier, 90)]

        optimize.optimize_partitions(self.clickhouse, self.database, self.table, parts)

        # all parts should be optimized
        parts = optimize.get_partitions_to_optimize(self.clickhouse, self.database, self.table)
        assert parts == []
