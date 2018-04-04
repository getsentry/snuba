from hashlib import md5

from clickhouse_driver import Client

from snuba import settings, util
from snuba.processor import process_raw_event
from snuba.writer import row_from_processed_event, write_rows


class BaseTest(object):
    def setup_method(self, test_method):
        assert settings.TESTING == True, "settings.TESTING is False, try `SNUBA_SETTINGS=test` or `make test`"

        from fixtures import raw_event

        self.event = self.wrap_raw_event(raw_event)

        self.table = 'test'
        self.conn = Client('localhost')
        self.conn.execute("DROP TABLE IF EXISTS %s" % self.table)
        self.conn.execute(util.get_table_definition('test', 'Memory', settings.SCHEMA_COLUMNS))

    def teardown_method(self, test_method):
        self.conn.execute("DROP TABLE IF EXISTS %s" % self.table)
        self.conn.disconnect()

    def wrap_raw_event(self, event):
        "Wrap a raw event like the Sentry codebase does before sending to Kafka."

        unique = "%s:%s" % (str(event['project']), event['id'])
        primary_hash = md5(unique).hexdigest()

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
            processed = process_raw_event(event)
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

        write_rows(self.conn, table=self.table, columns=settings.WRITER_COLUMNS, rows=rows)
