import logging

from snuba.clickhouse import ALL_COLUMNS, Array


logger = logging.getLogger('snuba.writer')


def row_from_processed_event(event, columns=ALL_COLUMNS):
    values = []
    for col in columns:
        value = event.get(col.flattened, None)
        if value is None and isinstance(col.type, Array):
            value = _create_missing_array(col.flattened, event)
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
    # This is plain array, not a nested column.
    if '.' not in colname:
        return []

    prefix, _ = colname.split('.', 1)
    prefix += '.'

    for key in event.keys():
        if key != colname and key.startswith(prefix):
            return [None] * len(event[key])

    # no siblings, empty array is safe!
    return []


def write_rows(connection, table, rows, types_check=False, columns=ALL_COLUMNS):
    connection.execute_robust("""
        INSERT INTO %(table)s (%(colnames)s) VALUES""" % {
        'colnames': ", ".join(col.escaped for col in columns),
        'table': table,
    }, rows, types_check=types_check)
