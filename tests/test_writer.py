import collections
import uuid
from datetime import datetime
import pytest
from clickhouse_driver import Client

from snuba import settings


class TestWriter(object):
    def setup_method(self, test_method):
        self.table = str(uuid.uuid4())
        self.conn = Client('localhost')
        self.conn.execute(settings.get_local_table_definition(self.table))

    def teardown_method(self, test_method):
        self.conn.execute("DROP TABLE %s" % self.table)
        self.conn.disconnect()

    def _write_events(self, events):
        rows = []

        for event in events:
            for colname in settings.WRITER_COLUMNS:
                value = event.get(colname, None)

                # Hack to handle default value for array columns
                if value is None and '.' in colname:
                    value = []

                rows.append(value)

        return self._write_rows(rows)

    def _write_rows(self, rows):
        if not isinstance(rows, collections.Iterable):
            rows = [rows]

        self.conn.execute("""
            INSERT INTO %(table)s (
                %(colnames)s
            ) VALUES
            """ % {
                'colnames': ", ".join(settings.WRITER_COLUMNS),
                'table': self.table,
            }, rows)

    def test_foo(self):
        # only the required fields
        events = [{
            'event_id': 'x' * 32,
            'timestamp': datetime.now(),
            'platform': 'foo',
            'message': 'foo',
            'primary_hash': 'x' * 16,
            'project_id': 1,
            'received': datetime.now(),
        }]

        self._write_events(events)

        res = self.conn.execute("SELECT count() FROM %s" % self.table)
        print(res)
