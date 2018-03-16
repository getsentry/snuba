import collections
from hashlib import md5

from clickhouse_driver import Client

from snuba import settings


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

    def _write_events(self, events):
        if not isinstance(events, collections.Iterable):
            events = [events]

        rows = []
        for event in events:
            row = []
            for colname in settings.WRITER_COLUMNS:
                value = event.get(colname, None)

                # Hack to handle default value for array columns
                if value is None and '.' in colname:
                    value = []

                row.append(value)
            rows.append(row)

        return self._write_rows(rows)

    def _write_rows(self, rows):
        if not isinstance(rows, collections.Iterable):
            rows = [rows]

        self.conn.execute("""
            INSERT INTO %(table)s (%(colnames)s) VALUES""" % {
            'colnames': ", ".join(settings.WRITER_COLUMNS),
            'table': self.table,
        }, rows)
