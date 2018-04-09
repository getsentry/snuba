import random
from datetime import datetime

from snuba import settings


def row_from_processed_event(event, columns=settings.WRITER_COLUMNS):
    # TODO: clickhouse-driver expects datetimes, would be nice to skip this
    event['timestamp'] = datetime.utcfromtimestamp(event['timestamp'])
    event['received'] = datetime.utcfromtimestamp(event['received'])

    values = []
    for colname in columns:
        value = event.get(colname, None)

        # Hack to handle default value for array columns
        if value is None and '.' in colname:
            value = []

        values.append(value)

    return values


def write_rows(connection, table, columns, rows):
    connection.execute("""
        INSERT INTO %(table)s (%(colnames)s) VALUES""" % {
        'colnames': ", ".join(columns),
        'table': table,
    }, rows)


class SnubaWriter(object):
    def __init__(self, connections, columns, table):
        self.connections = connections
        self.columns = columns
        self.table = table

    def get_connection(self):
        return random.choice(self.connections)

    def write(self, rows):
        return write_rows(self.get_connection(), self.table, self.columns, rows)
