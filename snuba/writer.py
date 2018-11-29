import logging

from snuba.clickhouse import ALL_COLUMNS, Array


logger = logging.getLogger('snuba.writer')


def row_from_processed_event(event, columns=ALL_COLUMNS):
    values = []
    for col in columns:
        value = event.get(col.flattened, None)
        if value is None and isinstance(col.type, Array):
            value = []
        values.append(value)

    return values


def write_rows(connection, table, rows, types_check=False, columns=ALL_COLUMNS):
    connection.execute_robust("""
        INSERT INTO %(table)s (%(colnames)s) VALUES""" % {
        'colnames': ", ".join(col.escaped for col in columns),
        'table': table,
    }, rows, types_check=types_check)
