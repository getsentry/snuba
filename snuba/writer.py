import json
import logging
from datetime import datetime

from urllib3.connectionpool import HTTPConnectionPool
from urllib3.exceptions import HTTPError
from urllib.parse import urlencode, urljoin

from snuba.clickhouse import DATETIME_FORMAT, Array


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


class HTTPBatchWriter(BatchWriter):
    def __init__(self, schema, host, port, options=None):
        self.__schema = schema
        self.__pool = HTTPConnectionPool(host, port)
        self.__options = options if options is not None else {}

    def __default(self, value):
        if isinstance(value, datetime):
            return value.strftime(DATETIME_FORMAT)
        else:
            raise TypeError

    def __encode(self, row):
        return json.dumps(row, default=self.__default).encode('utf-8')

    def write(self, rows):
        parameters = self.__options.copy()
        parameters['query'] = f"INSERT INTO {self.__schema.get_table_name()} FORMAT JSONEachRow"
        resp = self.__pool.urlopen(
            'POST',
            '/?' + urlencode(parameters),
            headers={
                'Connection': 'keep-alive',
                'Accept-Encoding': 'gzip,deflate',
            },
            body=map(self.__encode, rows),
            chunked=True,
        )
        if resp.status//100 != 2:
            raise HTTPError(f"{resp.status} Unexpected")
