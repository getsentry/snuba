import json
import logging
import re
from datetime import datetime
from typing import Any, Iterable, List, Mapping

from urllib3.connectionpool import HTTPConnectionPool
from urllib3.exceptions import HTTPError
from urllib.parse import urlencode, urljoin

from snuba.clickhouse import DATETIME_FORMAT, Array


logger = logging.getLogger("snuba.writer")

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
        self.__connection.execute_robust(
            "INSERT INTO %(table)s (%(colnames)s) VALUES"
            % {
                "colnames": ", ".join(col.escaped for col in columns),
                "table": self.__schema.get_table_name(),
            },
            [self.__row_to_column_list(columns, row) for row in rows],
            types_check=False,
        )


class ClickHouseError(Exception):
    def __init__(self, code: int, message: str):
        self.code = code
        self.message = message

    def __str__(self) -> str:
        return f"[{self.code}] {self.message}"

    def __repr__(self) -> str:
        return f"<{type(self).__name__}: {self}>"


CLICKHOUSE_ERROR_RE = re.compile(
    r"^Code: (?P<code>\d+), e.displayText\(\) = DB::Exception: (?P<message>.+)$"
)


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

    def __encode(self, row: WriterTableRow) -> bytes:
        return json.dumps(row, default=self.__default).encode("utf-8")

    def write(self, rows: Iterable[WriterTableRow]):
        response = self.__pool.urlopen(
            "POST",
            "/?"
            + urlencode(
                {
                    **self.__options,
                    "query": f"INSERT INTO {self.__table_name} FORMAT JSONEachRow",
                }
            ),
            headers={"Connection": "keep-alive", "Accept-Encoding": "gzip,deflate"},
            body=map(self.__encode, rows),
            chunked=True,
        )

        if response.status != 200:
            # XXX: This should be switched to just parse the JSON body after
            # https://github.com/yandex/ClickHouse/issues/6272 is available.
            details = CLICKHOUSE_ERROR_RE.match(response.data.decode("utf8"))
            if details is not None:
                code, message = details.groups()
                raise ClickHouseError(int(code), message)
            else:
                raise HTTPError(f"{response.status} Unexpected")


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
