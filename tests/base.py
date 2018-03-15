import collections

from clickhouse_driver import Client

from snuba import settings


class BaseTest(object):
    def setup_method(self, test_method):
        self.table = 'test'
        self.conn = Client('localhost')
        self.conn.execute("""
            CREATE TABLE %(table)s (%(columns)s) ENGINE = Memory""" % {
                'table': self.table, 'columns': settings.COLUMNS
            }
        )

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
