import logging

from snuba.clickhouse import Array


logger = logging.getLogger('snuba.writer')


def row_from_processed_event(schema, event):
    values = []
    for col in schema.ALL_COLUMNS:
        value = event.get(col.flattened, None)
        if value is None and isinstance(col.type, Array):
            value = []
        values.append(value)

    return values


def write_rows(connection, dataset, rows, types_check=False):
    connection.execute_robust(
        """
        INSERT INTO %(table)s (%(colnames)s) VALUES"""
        % {
            'colnames': ", ".join(col.escaped for col in dataset.SCHEMA.ALL_COLUMNS),
            'table': dataset.SCHEMA.QUERY_TABLE,
        },
        rows,
        types_check=types_check,
    )
