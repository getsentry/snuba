import json
import random

from datetime import datetime
from clickhouse_driver import Client
from kafka import KafkaConsumer

import settings




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
            INSERT INTO %(table)s (
                %(colnames)s
            ) VALUES
            """ % {
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


def run():
    connections = [Client(node) for node in settings.CLICKHOUSE_NODES]

    # ensure tables exist
    for conn in connections:
        conn.execute(settings.get_local_table_definition())
        conn.execute(settings.DIST_TABLE_DEFINITION)

    consumer = KafkaConsumer(
        settings.WRITER_TOPIC,
        bootstrap_servers=settings.BROKERS,
        group_id=settings.WRITER_CONSUMER_GROUP,
    )

    writer = SnubaWriter(connections=connections)
    for msg in consumer:
        writer.process_row(json.loads(msg.value))


if __name__ == '__main__':
    run()
