import json
import random

from datetime import datetime

from snuba import settings


class SnubaWriter(object):
    def __init__(self, connections, batch_size=settings.WRITER_BATCH_SIZE):
        self.connections = connections
        self.batch_size = batch_size
        self.clear_batch()

    def clear_batch(self):
        self.batch = []

    def should_flush(self):
        return len(self.batch) >= self.batch_size

    def get_connection(self):
        return random.choice(self.connections)

    def flush(self):
        conn = self.get_connection()

        conn.execute("""
            INSERT INTO %(table)s (%(colnames)s) VALUES""" % {
                'colnames': ", ".join(settings.WRITER_COLUMNS),
                'table': settings.DIST_TABLE
            }, self.batch)
        self.clear_batch()

    def process_row(self, row):
        # TODO: clickhouse-driver expects datetimes, would be nice to skip this
        row['timestamp'] = datetime.fromtimestamp(row['timestamp'])
        row['received'] = datetime.fromtimestamp(row['received'])

        values = []
        for colname in settings.WRITER_COLUMNS:
            value = row.get(colname, None)

            # Hack to handle default value for array columns
            if value is None and '.' in colname:
                value = []

            values.append(value)

        self.batch.append(values)

        if self.should_flush():
            self.flush()
