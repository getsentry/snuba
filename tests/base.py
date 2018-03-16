from hashlib import md5

from clickhouse_driver import Client

from snuba import settings
from snuba.processor import process_raw_event
from snuba.writer import row_from_processed_event


class BaseTest(object):
    def setup_method(self, test_method):
        from fixtures import raw_event

        self.base_event = self.wrap_raw_event(raw_event)

        self.table = 'test'
        self.conn = Client('localhost')
        self.conn.execute("""
            CREATE TABLE %(table)s (%(columns)s) ENGINE = Memory""" % {
            'table': self.table, 'columns': settings.COLUMNS
        })

    def wrap_raw_event(self, event):
        "Wrap a raw event like the Sentry codebase does before sending to Kafka."

        unique = "%s:%s" % (str(event['project']), event['id'])
        primary_hash = md5(unique).hexdigest()[:16]

        return {
            'event_id': event['id'],
            'primary_hash': primary_hash,
            'project_id': event['project'],
            'message': event['message'],
            'platform': event['platform'],
            'datetime': event['datetime'],
            'data': event
        }

    def teardown_method(self, test_method):
        self.conn.execute("DROP TABLE %s" % self.table)
        self.conn.disconnect()

    def write_raw_events(self, events):
        if not isinstance(events, (list, tuple)):
            events = [events]

        processed = []
        for event in events:
            processed.append(process_raw_event(event))

        return self.write_processed_events(processed)

    def write_processed_events(self, events):
        if not isinstance(events, (list, tuple)):
            events = [events]

        rows = []
        for event in events:
            rows.append(row_from_processed_event(event, settings.WRITER_COLUMNS))

        return self.write_rows(rows)

    def write_rows(self, rows):
        if not isinstance(rows, (list, tuple)):
            rows = [rows]

        self.conn.execute("""
            INSERT INTO %(table)s (%(colnames)s) VALUES""" % {
            'colnames': ", ".join(settings.WRITER_COLUMNS),
            'table': self.table,
        }, rows)
