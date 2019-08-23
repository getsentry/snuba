import json
import logging
from datetime import datetime
from typing import Any, Iterable, List, Mapping

from urllib3.connectionpool import HTTPConnectionPool
from urllib3.exceptions import HTTPError
from urllib.parse import urlencode, urljoin

from snuba.clickhouse import DATETIME_FORMAT, Array


logger = logging.getLogger('snuba.writer')

WriterTableRow = Mapping[str, Any]


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

    def write(self, rows: Iterable[WriterTableRow]):
        columns = self.__schema.get_columns()
        self.__connection.execute_robust("INSERT INTO %(table)s (%(colnames)s) VALUES" % {
            'colnames': ", ".join(col.escaped for col in columns),
            'table': self.__schema.get_table_name(),
        }, [self.__row_to_column_list(columns, row) for row in rows], types_check=False)


class HTTPBatchWriter(BatchWriter):
    def __init__(self, schema, host, port, options=None, table_name=None):
        self.__schema = schema
        self.__pool = HTTPConnectionPool(host, port)
        self.__options = options if options is not None else {}
        self.__table_name = table_name or schema.get_table_name()

    def __default(self, value):
        if isinstance(value, datetime):
            return value.strftime(DATETIME_FORMAT)
        else:
            raise TypeError

    def __encode(self, row: WriterTableRow):
        return json.dumps(row, default=self.__default).encode('utf-8')

    def write(self, rows: Iterable[WriterTableRow]):
        parameters = self.__options.copy()
        parameters['query'] = f"INSERT INTO {self.__table_name} FORMAT JSONEachRow"
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
        if resp.status // 100 != 2:
            raise HTTPError(f"{resp.status} Unexpected")


class BufferedWriterWrapper:
    """
    This is a wrapper that adds a buffer around a BatchWriter.
    When consuming data from Kafka, the buffering logic is performed by the
    batching consumer.
    This is for the use cases that are not Kafka related.

    This is not thread safe. Don't try to do parallel flush hoping in the GIL.
    """

    def __init__(self, writer: BatchWriter, buffer_size: int):
        self.__writer = writer
        self.__buffer_size = buffer_size
        self.__buffer: List[WriterTableRow] = []

    def __flush(self) -> None:
        logger.debug("Flushing buffer with %d elements", len(self.__buffer))
        self.__writer.write(self.__buffer)
        self.__buffer = []

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        if self.__buffer:
            self.__flush()

    def write(self, row: WriterTableRow):
        self.__buffer.append(row)
        if len(self.__buffer) >= self.__buffer_size:
            self.__flush()
