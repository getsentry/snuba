from datetime import datetime
import logging
import simplejson as json
import time

from clickhouse_driver import errors

from snuba import settings
from snuba.consumer import AbstractBatchWorker


logger = logging.getLogger('snuba.writer')


def row_from_processed_event(event, columns=settings.WRITER_COLUMNS):
    # TODO: clickhouse-driver expects datetimes, would be nice to skip this
    timestamp = datetime.utcfromtimestamp(event['timestamp'])
    event['timestamp'] = timestamp
    event['date'] = timestamp.date()
    if event.get('received'):
        event['received'] = datetime.utcfromtimestamp(event['received'])

    values = []
    for colname in columns:
        value = event.get(colname, None)

        # Hack to handle default value for array columns
        if value is None and '.' in colname:
            value = []

        values.append(value)

    return values


def write_rows(connection, table, columns, rows, types_check=False):
    connection.execute("""
        INSERT INTO %(table)s (%(colnames)s) VALUES""" % {
        'colnames': ", ".join('`{}`'.format(c) for c in columns),
        'table': table,
    }, rows, types_check=types_check)


class WriterWorker(AbstractBatchWorker):
    def __init__(self, clickhouse, table_name):
        self.clickhouse = clickhouse
        self.table_name = table_name

    def process_message(self, message):
        return row_from_processed_event(json.loads(message.value()))

    def flush_batch(self, batch):
        retries = 3
        while True:
            try:
                with self.clickhouse as ch:
                    write_rows(ch, self.table_name, settings.WRITER_COLUMNS, batch)

                break  # success
            except (errors.NetworkError, errors.SocketTimeoutError) as e:
                logger.warning("Write to Clickhouse failed: %s (%d retries)" % (str(e), retries))
                if retries <= 0:
                    raise
                retries -= 1
                time.sleep(1)
                continue
            except errors.ServerException as e:
                logger.warning("Write to Clickhouse failed: %s (retrying)" % str(e))
                if e.code == errors.ErrorCodes.TOO_MUCH_SIMULTANEOUS_QUERIES:
                    time.sleep(1)
                    continue
                else:
                    raise

    def shutdown(self):
        pass
