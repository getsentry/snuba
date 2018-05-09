from datetime import datetime

from snuba import settings


def row_from_processed_event(event, columns=settings.WRITER_COLUMNS):
    # TODO: clickhouse-driver expects datetimes, would be nice to skip this
    event['timestamp'] = datetime.utcfromtimestamp(event['timestamp'])
    event['received'] = datetime.utcfromtimestamp(event['received'])

    if not event.get('deleted'):
        event['deleted'] = 0

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
        'colnames': ", ".join(columns),
        'table': table,
    }, rows, types_check=types_check)
