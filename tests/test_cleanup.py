import uuid
from datetime import datetime, timedelta
from typing import Any, Mapping

from snuba import cleanup, settings
from snuba.clusters.cluster import ClickhouseClientSettings
from snuba.datasets.storages import StorageKey
from snuba.datasets.storages.factory import get_writable_storage
from tests.base import BaseDatasetTest


class TestCleanup(BaseDatasetTest):
    def setup_method(self, test_method):
        super(TestCleanup, self).setup_method(test_method, "events")

    def test(self) -> None:
        def to_monday(d: datetime) -> datetime:
            return d - timedelta(days=d.weekday())

        base = datetime(1999, 12, 26)  # a sunday

        storage = get_writable_storage(StorageKey.EVENTS)
        clickhouse = storage.get_cluster().get_query_connection(
            ClickhouseClientSettings.CLEANUP
        )

        table = storage.get_table_writer().get_schema().get_table_name()

        parts = cleanup.get_active_partitions(clickhouse, self.database, table)
        assert parts == []

        # base, 90 retention
        self.write_rows([self.create_event_row_for_date(base)])
        parts = cleanup.get_active_partitions(clickhouse, self.database, table)
        assert parts == [(to_monday(base), 90)]
        stale = cleanup.filter_stale_partitions(parts, as_of=base)
        assert stale == []

        # -40 days, 90 retention
        three_weeks_ago = base - timedelta(days=7 * 3)
        self.write_rows([self.create_event_row_for_date(three_weeks_ago)])
        parts = cleanup.get_active_partitions(clickhouse, self.database, table)
        assert parts == [(to_monday(three_weeks_ago), 90), (to_monday(base), 90)]
        stale = cleanup.filter_stale_partitions(parts, as_of=base)
        assert stale == []

        # -100 days, 90 retention
        thirteen_weeks_ago = base - timedelta(days=7 * 13)
        self.write_rows([self.create_event_row_for_date(thirteen_weeks_ago)])
        parts = cleanup.get_active_partitions(clickhouse, self.database, table)
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
        parts = cleanup.get_active_partitions(clickhouse, self.database, table)
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
        parts = cleanup.get_active_partitions(clickhouse, self.database, table)
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

        cleanup.drop_partitions(clickhouse, self.database, table, stale, dry_run=False)

        parts = cleanup.get_active_partitions(clickhouse, self.database, table)
        assert parts == [
            (to_monday(three_weeks_ago), 90),
            (to_monday(one_week_ago), 30),
            (to_monday(base), 90),
        ]

    def create_event_row_for_date(
        self, dt: datetime, retention_days: int = settings.DEFAULT_RETENTION_DAYS
    ) -> Mapping[str, Any]:
        return {
            "event_id": uuid.uuid4().hex,
            "project_id": 1,
            "group_id": 1,
            "deleted": 0,
            "timestamp": dt,
            "retention_days": retention_days,
        }
