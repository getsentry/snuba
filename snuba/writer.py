import logging

from snuba.clickhouse import Array


logger = logging.getLogger('snuba.writer')


class BatchWriter(object):
    def __init__(self, schema):
        raise NotImplementedError

    def write(self, rows):
        raise NotImplementedError


class NativeDriverBatchWriter(BatchWriter):
    def __init__(self, schema, connection):
        self.__schema = schema
        self.__connection = connection

    def __row_to_column_list(self, columns, row):
        values = []
        for col in columns:
            value = row.get(col.flattened, None)
            if value is None and isinstance(col.type, Array):
                value = []
            values.append(value)
        return values

    def write(self, rows):
        columns = self.__schema.get_columns()
        self.__connection.execute_robust("INSERT INTO %(table)s (%(colnames)s) VALUES" % {
            'colnames': ", ".join(col.escaped for col in columns),
            'table': self.__schema.get_table_name(),
        }, [self.__row_to_column_list(columns, row) for row in rows], types_check=False)
