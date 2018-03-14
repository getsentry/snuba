import json
import random

from datetime import datetime
from clickhouse_driver import Client
from kafka import KafkaConsumer

import settings


connections = [Client(node) for node in settings.CLICKHOUSE_NODES]

for conn in connections:
    conn.execute(settings.LOCAL_TABLE_DEFINITION)
    conn.execute(settings.DIST_TABLE_DEFINITION)


consumer = KafkaConsumer(
    settings.WRITER_TOPIC,
    bootstrap_servers=settings.BROKERS,
    group_id=settings.WRITER_CONSUMER_GROUP,
)


class SnubaWriter(object):
    def __init__(self, batch_size=settings.WRITER_BATCH_SIZE):
        self.batch_size = batch_size
        self.clear_batch()

    def clear_batch(self):
        self.batch = []

    def should_flush(self):
        return len(self.batch) >= self.batch_size

    def get_connection(self):
        return random.choice(connections)

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
            values.append(row.get(colname, None))

        self.batch.append(values)

        if self.should_flush():
            self.flush()


writer = SnubaWriter()
for msg in consumer:
    writer.process_row(json.loads(msg.value))
