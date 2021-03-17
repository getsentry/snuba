from __future__ import annotations

import logging
import re
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from queue import Queue, SimpleQueue
from typing import Any, Sequence, cast, Iterable, Iterator, Mapping, Optional, Union
from urllib.parse import urlencode

import rapidjson
from urllib3.connectionpool import HTTPConnectionPool
from urllib3.exceptions import HTTPError

from snuba import settings
from snuba.clickhouse import DATETIME_FORMAT
from snuba.clickhouse.errors import ClickhouseWriterError
from snuba.utils.codecs import Encoder
from snuba.utils.iterators import chunked
from snuba.utils.metrics import MetricsBackend
from snuba.writer import BatchWriter, WriterTableRow


logger = logging.getLogger(__name__)


CLICKHOUSE_ERROR_RE = re.compile(
    r"^Code: (?P<code>\d+), e.displayText\(\) = (?P<type>(?:\w+)::(?:\w+)): (?P<message>.+?(?: \(at row (?P<row>\d+)\))?)$",
    re.MULTILINE,
)


JSONRow = bytes  # a single row in JSONEachRow format


class JSONRowEncoder(Encoder[bytes, WriterTableRow]):
    def __default(self, value: Any) -> Any:
        if isinstance(value, datetime):
            return value.strftime(DATETIME_FORMAT)
        else:
            raise TypeError

    def encode(self, value: WriterTableRow) -> JSONRow:
        return cast(
            bytes, rapidjson.dumps(value, default=self.__default).encode("utf-8")
        )


class InsertStatement:
    def __init__(self, table_name: str) -> None:
        self.__table_name = table_name
        self.__database: Optional[str] = None
        self.__format: Optional[str] = None
        self.__column_names: Optional[Sequence[str]] = None

    def with_database(self, database_name: str) -> InsertStatement:
        self.__database = database_name
        return self

    def with_format(self, format: str) -> InsertStatement:
        self.__format = format
        return self

    def with_columns(self, column_names: Sequence[str]) -> InsertStatement:
        self.__column_names = column_names
        return self

    def get_qualified_table(self) -> str:
        return (
            f"{self.__database}.{self.__table_name}"
            if self.__database
            else self.__table_name
        )

    def build_statement(self) -> str:
        columns_statement = (
            f"({','.join(self.__column_names)}) " if self.__column_names else ""
        )
        format_statement = f" FORMAT {self.__format}" if self.__format else ""
        return f"INSERT INTO {self.__database}.{self.__table_name} {columns_statement} {format_statement}"


class HTTPWriteBatch:
    """
    Sends a batch of binary data as payload of a Clickhouse insert statement.
    It takes care of issuing the HTTP request and parsing the response.

    Data is provided in blocks of data (expressed as bytes). Each call to
    `append` adds a block to the batch.
    One HTTP request is created at the initialization and remains alive for
    the lifetime of the object itself.
    In order to issue a separate INSERT, a new instance has to be created.

    Batches can be broken into chunks by defining the chunk size. This drives
    the size of each element of the iterable that is provided to the
    `urlopen` method.
    Example:
    Values appended:
    [ v1, v2, v3, v4]
    If chunk size is 2, these values will be sent:
    [v1, v2] [v3, v4]

    Batches can be partially or entirely buffered locally. This behavior
    is controlled by `buffer_size`.
    Setting buffer_size to 0 means all the values are enqueued locally
    while they are sent to the server. `append` would not block in this
    case.
    If the buffer size is higher and the buffer is full, the `append`
    command will block while the value is sent to the server.
    """

    def __init__(
        self,
        executor: ThreadPoolExecutor,
        pool: HTTPConnectionPool,
        user: str,
        password: str,
        statement: InsertStatement,
        encoding: Optional[str],
        buffer_size: int,  # 0 means unbounded
        options: Mapping[str, Any],  # should be ``Mapping[str, str]``?
        chunk_size: Optional[int] = None,
    ) -> None:
        if chunk_size is None:
            chunk_size = settings.CLICKHOUSE_HTTP_CHUNK_SIZE

        self.__queue: Union[
            Queue[Union[bytes, None]], SimpleQueue[Union[bytes, None]]
        ] = Queue(buffer_size) if buffer_size else SimpleQueue()

        body = self.__read_until_eof()
        if chunk_size > 1:
            body = (b"".join(chunk) for chunk in chunked(body, chunk_size))
        elif not chunk_size > 0:
            raise ValueError("chunk size must be greater than zero")

        encoding_header = {"Content-Encoding": encoding} if encoding else {}

        self.__result = executor.submit(
            pool.urlopen,
            "POST",
            "/?" + urlencode({**options, "query": statement.build_statement()}),
            headers={
                "X-ClickHouse-User": user,
                "X-ClickHouse-Key": password,
                "Connection": "keep-alive",
                "Accept-Encoding": "gzip,deflate",
                **encoding_header,
            },
            body=body,
        )

        self.__rows = 0
        self.__size = 0
        self.__closed = False

    def __repr__(self) -> str:
        return f"<{type(self).__name__}: {self.__rows} rows ({self.__size} bytes)>"

    def __read_until_eof(self) -> Iterator[bytes]:
        while True:
            value = self.__queue.get()
            if value is None:
                logger.debug("Finished sending data from %r.", self)
                return

            yield value

    def append(self, value: bytes) -> None:
        assert not self.__closed

        self.__queue.put(value)
        self.__rows += 1
        self.__size += len(value)

    def close(self) -> None:
        self.__queue.put(None)
        self.__closed = True

    def join(self, timeout: Optional[float] = None) -> None:
        response = self.__result.result(timeout)
        logger.debug("Received response for %r.", self)

        if response.status != 200:
            # XXX: This should be switched to just parse the JSON body after
            # https://github.com/yandex/ClickHouse/issues/6272 is available.
            content = response.data.decode("utf8")
            details = CLICKHOUSE_ERROR_RE.match(content)
            if details is not None:
                code = int(details["code"])
                message = details["message"]
                row = int(details["row"]) if details["row"] is not None else None
                raise ClickhouseWriterError(code, message, row)
            else:
                raise HTTPError(
                    f"Received unexpected {response.status} response: {content}"
                )


class HTTPBatchWriter(BatchWriter[bytes]):
    def __init__(
        self,
        host: str,
        port: int,
        user: str,
        password: str,
        metrics: MetricsBackend,  # deprecated
        statement: InsertStatement,
        encoding: Optional[str],
        options: Optional[Mapping[str, Any]] = None,
        chunk_size: Optional[int] = None,
        buffer_size: int = 0,
    ):
        self.__pool = HTTPConnectionPool(host, port)
        self.__executor = ThreadPoolExecutor()

        self.__options = options if options is not None else {}
        self.__user = user
        self.__password = password
        self.__encoding = encoding
        self.__statement = statement
        self.__buffer_size = buffer_size
        self.__chunk_size = chunk_size

    def __repr__(self) -> str:
        return f"<{type(self).__name__}: {self.__statement.get_qualified_table()} on {self.__pool.host}:{self.__pool.port}>"

    def write(self, values: Iterable[bytes]) -> None:
        batch = HTTPWriteBatch(
            self.__executor,
            self.__pool,
            self.__user,
            self.__password,
            self.__statement,
            self.__encoding,
            self.__buffer_size,
            self.__options,
            self.__chunk_size,
        )

        for value in values:
            batch.append(value)

        batch.close()
        batch.join()
