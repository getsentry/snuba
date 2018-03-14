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
    def __init__(self, batch_size=settings.BATCH_SIZE):
        self.batch_size = batch_size
        self.clear_batch()

    def clear_batch(self):
        self.batch = []

    def should_flush(self):
        return len(self.batch) >= self.batch_size

    def get_connection(self):
        return random.choice(connections)

    def process_row(self, row):
        # TODO: this sucks
        row = list(row)
        row[1] = datetime.fromtimestamp(row[1])
        row[6] = datetime.fromtimestamp(row[6])

        self.batch.append(row)

        if self.should_flush():
            conn = self.get_connection()
            conn.execute("""
            INSERT INTO %(table)s (
                event_id,
                timestamp,
                platform,
                message,
                primary_hash,
                project_id,
                received,
                user_id,
                username,
                email,
                ip_address,
                sdk_name,
                sdk_version,
                level,
                logger,
                server_name,
                transaction,
                environment,
                release,
                dist,
                site,
                url,
                tags.key,
                tags.value,
                http_method,
                http_referer,
                exception_stacks.type,
                exception_stacks.value,
                exception_frames.abs_path,
                exception_frames.filename,
                exception_frames.package,
                exception_frames.module,
                exception_frames.function,
                exception_frames.in_app,
                exception_frames.colno,
                exception_frames.lineno,
                exception_frames.stack_level
            ) VALUES
            """ % {'table': settings.DIST_TABLE}, self.batch)
            self.clear_batch()


writer = SnubaWriter()
for msg in consumer:
    writer.process_row(json.loads(msg.value))
