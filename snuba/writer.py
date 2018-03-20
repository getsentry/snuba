import random

from datetime import datetime

from snuba import settings


def row_from_processed_event(event, columns):
    # TODO: clickhouse-driver expects datetimes, would be nice to skip this
    event['timestamp'] = datetime.fromtimestamp(event['timestamp'])
    event['received'] = datetime.fromtimestamp(event['received'])

    values = []
    for colname in columns:
        value = event.get(colname, None)

        # Hack to handle default value for array columns
        if value is None and '.' in colname:
            value = []

        values.append(value)

    return values


class SnubaWriter(object):
    def __init__(self, connections, columns=settings.WRITER_COLUMNS, table=settings.DIST_TABLE):
        self.connections = connections
        self.columns = columns
        self.table = table

    def get_connection(self):
        return random.choice(self.connections)

    def write(self, rows):
        conn = self.get_connection()
        conn.execute("""
            INSERT INTO %(table)s (%(colnames)s) VALUES""" % {
            'colnames': ", ".join(self.columns),
            'table': self.table,
        }, rows)
