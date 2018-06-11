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
    event['timestamp'] = datetime.utcfromtimestamp(event['timestamp'])
    if event.get('received'):
        event['received'] = datetime.utcfromtimestamp(event['received'])

    values = []
    for colname in columns:
        value = event.get(colname, None)
        if value is None and '.' in colname:
            value = _create_missing_array(colname, event)
        values.append(value)

    return values


def _create_missing_array(colname, event):
    """
    ClickHouse `Nested` columns are implemented as arrays and sibling columns
    must have the same length as one another. The documentation states:
    > During insertion, the system checks that they have the same length.
    But as of this writing, this doesn't seem to be true:
    https://github.com/yandex/ClickHouse/issues/2231

    When a new `Nested` column is added to the schema, it may be missing
    from pre-existing events in the processed data topic. We need
    to write arrays of the same length as the sibling columns, so we
    look at the event for the first sibling column and use its length
    (since all siblings will have the same length).

    It's important to note that this is (1) not fast and (2) only done
    temporarily against processed events that are missing the new column.
    Once the processor is updated and the writer moves on to those new
    events, this method will not be called.
    """
    prefix, _ = colname.split('.', 1)
    prefix += '.'

    for key in event.keys():
        if key.startswith(prefix):
            return [None] * len(event[key])

    # no siblings, empty array is safe!
    return []


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
                write_rows(self.clickhouse, self.table_name, settings.WRITER_COLUMNS, batch)

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
