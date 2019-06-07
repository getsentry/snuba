import logging

logger = logging.getLogger('snuba.writer')


def write_rows(connection, dataset, rows, types_check=False, columns=None):
    if columns is None:
        columns = dataset.get_schema().get_columns()
    table = dataset.get_schema().get_table_name()
    connection.execute_robust("""
        INSERT INTO %(table)s (%(colnames)s) VALUES""" % {
        'colnames': ", ".join(col.escaped for col in columns),
        'table': table,
    }, rows, types_check=types_check)
