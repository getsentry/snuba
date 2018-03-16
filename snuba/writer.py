import random

from datetime import datetime

from snuba import settings


def row_from_processed_event(event, columns):
    values = []
    for colname in columns:
        value = event.get(colname, None)

        # Hack to handle default value for array columns
        if value is None and '.' in colname:
            value = []

        values.append(value)

    return values


class SnubaWriter(object):
    def __init__(self, connections, batch_size=settings.WRITER_BATCH_SIZE,
                 columns=settings.WRITER_COLUMNS):
        self.connections = connections
        self.batch_size = batch_size
        self.columns = columns

        self.clear_batch()

    def clear_batch(self):
        self.batch = []

    def should_flush(self):
        return len(self.batch) >= self.batch_size

    def get_connection(self):
        return random.choice(self.connections)

    def flush(self):
        if len(self.batch) < 1:
            return

        conn = self.get_connection()
        conn.execute("""
            INSERT INTO %(table)s (%(colnames)s) VALUES""" % {
            'colnames': ", ".join(self.columns),
            'table': settings.DIST_TABLE
        }, self.batch)

        self.clear_batch()

    def write(self, event):
        # TODO: clickhouse-driver expects datetimes, would be nice to skip this
        event['timestamp'] = datetime.fromtimestamp(event['timestamp'])
        event['received'] = datetime.fromtimestamp(event['received'])

        row = row_from_processed_event(event, self.columns)

        self.batch.append(row)

        if self.should_flush():
            self.flush()
