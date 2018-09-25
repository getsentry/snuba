import calendar
from hashlib import md5
from datetime import datetime, timedelta
import uuid

from snuba import settings
from snuba.clickhouse import ClickhousePool, get_table_definition, get_test_engine
from snuba.processor import process_message
from snuba.writer import row_from_processed_event, write_rows


class BaseTest(object):
    def setup_method(self, test_method):
        assert settings.TESTING == True, "settings.TESTING is False, try `SNUBA_SETTINGS=test` or `make test`"

        from fixtures import raw_event

        timestamp = datetime.utcnow()
        raw_event['datetime'] = (timestamp - timedelta(seconds=2)).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        raw_event['received'] = int(calendar.timegm((timestamp - timedelta(seconds=1)).timetuple()))
        self.event = self.wrap_raw_event(raw_event)

        self.database = 'default'
        self.table = settings.CLICKHOUSE_TABLE

        self.clickhouse = ClickhousePool()

        self.clickhouse.execute("DROP TABLE IF EXISTS %s" % self.table)
        self.clickhouse.execute(
            get_table_definition(
                name=self.table,
                engine=get_test_engine(),
                columns=settings.SCHEMA_COLUMNS
            )
        )

    def create_event_for_date(self, dt, retention_days=settings.DEFAULT_RETENTION_DAYS):
        event = {
            'event_id': uuid.uuid4().hex,
            'project_id': 1,
            'deleted': 0,
        }
        event['timestamp'] = dt
        event['retention_days'] = retention_days
        return event

    def teardown_method(self, test_method):
        self.clickhouse.execute("DROP TABLE IF EXISTS %s" % self.table)

    def wrap_raw_event(self, event):
        "Wrap a raw event like the Sentry codebase does before sending to Kafka."

        unique = "%s:%s" % (str(event['project']), event['id'])
        primary_hash = md5(unique.encode('utf-8')).hexdigest()

        return {
            'event_id': event['id'],
            'primary_hash': primary_hash,
            'project_id': event['project'],
            'message': event['message'],
            'platform': event['platform'],
            'datetime': event['datetime'],
            'data': event
        }

    def write_raw_events(self, events):
        if not isinstance(events, (list, tuple)):
            events = [events]

        out = []
        for event in events:
            if 'primary_hash' not in event:
                event = self.wrap_raw_event(event)
            _, processed = process_message(event)
            out.append(processed)

        return self.write_processed_events(out)

    def write_processed_events(self, events):
        if not isinstance(events, (list, tuple)):
            events = [events]

        rows = []
        for event in events:
            rows.append(row_from_processed_event(event))

        return self.write_rows(rows)

    def write_rows(self, rows):
        if not isinstance(rows, (list, tuple)):
            rows = [rows]

        write_rows(self.clickhouse, table=self.table, columns=settings.WRITER_COLUMNS,
                   rows=rows, types_check=True)
