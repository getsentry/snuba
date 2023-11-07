from __future__ import annotations

import logging
import re
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from queue import Queue, SimpleQueue
from typing import (
    Any,
    Iterable,
    Iterator,
    List,
    Mapping,
    Optional,
    Sequence,
    Union,
    cast,
)
from urllib.parse import urlencode

import rapidjson
import sentry_sdk
from urllib3.connectionpool import HTTPConnectionPool
from urllib3.exceptions import HTTPError

from snuba import settings, state
from snuba.clickhouse import DATETIME_FORMAT
from snuba.clickhouse.errors import ClickhouseWriterError
from snuba.clickhouse.formatter.expression import ClickhouseExpressionFormatter
from snuba.clickhouse.query import Expression
from snuba.utils.codecs import Encoder
from snuba.utils.iterators import chunked
from snuba.utils.metrics import MetricsBackend
from snuba.writer import BatchWriter, WriterTableRow

logger = logging.getLogger(__name__)


CLICKHOUSE_ERROR_RE = re.compile(
    r"^Code: (?P<code>\d+), e.displayText\(\) = (?P<type>(?:\w+)::(?:\w+)): (?P<message>.+?(?: \(at row (?P<row>\d+)\))?)$",
    re.MULTILINE,
)

CLICKHOUSE_ERROR_RE_V2 = re.compile(
    r"^Code: (?P<code>\d+)\. (?P<message>.+?(?: \(at row (?P<row>\d+)\))?)$",
    re.MULTILINE,
)

JSONRow = bytes  # a single row in JSONEachRow format


class JSONRowEncoder(Encoder[bytes, WriterTableRow]):
    def __default(self, value: Any) -> Any:
        if isinstance(value, datetime):
            return value.strftime(DATETIME_FORMAT)
        else:
            raise TypeError

    def encode(self, value: WriterTableRow) -> bytes:
        return cast(
            bytes, rapidjson.dumps(value, default=self.__default).encode("utf-8")
        )


class ValuesRowEncoder(Encoder[bytes, WriterTableRow]):
    def __init__(self, columns: Iterable[str]) -> None:
        self.__columns = columns
        self.__formatter = ClickhouseExpressionFormatter()

    def encode_value(self, value: Any) -> str:
        if isinstance(value, Expression):
            return value.accept(self.__formatter)
        else:
            raise TypeError("unknown Clickhouse value type", value.__class__)

    def encode(self, row: WriterTableRow) -> bytes:
        ordered_columns = [
            self.encode_value(row.get(column)) for column in self.__columns
        ]
        ordered_columns_str = ",".join(ordered_columns)
        return f"({ordered_columns_str})".encode("utf-8")


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
        format_statement = ""

        if self.__format:
            if not self.__format == "VALUES":
                format_statement = f"FORMAT {self.__format}"
            else:
                format_statement = "VALUES"

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

    The "debug buffer" is being used to hold a prefix of the data
    stream in memory. If the error returned by clickhouse happens to point to a
    row contained in the first `debug_buffer_size_bytes` bytes of the data
    stream, the corresponding Sentry error will contain that row's data. The
    debug buffer is separate from the buffer that is being used to stream the
    HTTP.
    """

    def __init__(
        self,
        executor: ThreadPoolExecutor,
        pool: HTTPConnectionPool,
        metrics: MetricsBackend,
        user: str,
        password: str,
        statement: InsertStatement,
        encoding: Optional[str],
        options: Mapping[str, Any],  # should be ``Mapping[str, str]``?
        chunk_size: Optional[int] = None,
        buffer_size: int = 0,  # 0 means unbounded
        debug_buffer_size_bytes: Optional[int] = None,  # None means disabled
    ) -> None:
        if chunk_size is None:
            chunk_size = settings.CLICKHOUSE_HTTP_CHUNK_SIZE

        self.__queue: Union[
            Queue[Union[bytes, None]], SimpleQueue[Union[bytes, None]]
        ] = (Queue(buffer_size) if buffer_size else SimpleQueue())

        body = self.__read_until_eof()
        if chunk_size > 1:
            body = (b"".join(chunk) for chunk in chunked(body, chunk_size))
        elif not chunk_size > 0:
            raise ValueError("chunk size must be greater than zero")

        headers = {
            "X-ClickHouse-User": user,
            "Connection": "keep-alive",
            "Accept-Encoding": "gzip,deflate",
        }
        if password != "":
            headers["X-ClickHouse-Key"] = password
        if encoding:
            headers["Content-Encoding"] = encoding

        self._result = executor.submit(
            pool.urlopen,
            "POST",
            "/?" + urlencode({**options, "query": statement.build_statement()}),
            headers=headers,
            body=body,
        )

        self.__debug_buffer: List[bytes] = []
        self.__size = 0
        self.__debug_buffer_size_bytes = debug_buffer_size_bytes
        self.__closed = False

        self.__metrics = metrics
        self.__statement = statement

    def __repr__(self) -> str:
        return (
            f"<{type(self).__name__}: {self.__debug_buffer} rows ({self.__size} bytes)>"
        )

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
        self.__size += len(value)

        if self.__size < (self.__debug_buffer_size_bytes or 0):
            self.__debug_buffer.append(value)

    def close(self) -> None:
        self.__queue.put(None)
        self.__closed = True

    def join(self, timeout: Optional[float] = None) -> None:
        try:
            response = self._result.result(timeout)
        except TimeoutError:
            self.__metrics.increment(
                "http_batch.timeout",
                tags={"table": str(self.__statement.get_qualified_table())},
            )
            raise

        logger.debug("Received response for %r.", self)

        self.__metrics.timing(
            "http_batch.size",
            self.__size,
            tags={"table": str(self.__statement.get_qualified_table())},
        )

        if response.status != 200:
            # XXX: This should be switched to just parse the JSON body after
            # https://github.com/yandex/ClickHouse/issues/6272 is available.
            content = response.data.decode("utf8")
            details = CLICKHOUSE_ERROR_RE.match(content)
            if not details:
                # Error messages looks a bit different in newer versions e.g 22.8
                # so try a different way of parsing the content
                details = CLICKHOUSE_ERROR_RE_V2.match(content)
            if details is not None:
                code = int(details["code"])
                message = details["message"]
                row = int(details["row"]) if details["row"] is not None else None
                try:
                    if row is not None:
                        sentry_sdk.set_context(
                            "snuba_errored_row", {"data": self.__debug_buffer[row]}
                        )
                    sentry_sdk.set_tag("snuba_has_errored_row", "true")
                except IndexError:
                    sentry_sdk.set_context("snuba_errored_row", {"index_error": True})
                    sentry_sdk.set_tag("snuba_has_errored_row", "false")

                raise ClickhouseWriterError(message, code=code, row=row)
            else:
                raise HTTPError(
                    f"Received unexpected {response.status} response: {content}"
                )


class HTTPBatchWriter(BatchWriter[bytes]):
    def __init__(
        self,
        host: str,
        port: int,
        max_connections: int,
        block_connections: bool,
        user: str,
        password: str,
        metrics: MetricsBackend,
        statement: InsertStatement,
        encoding: Optional[str],
        options: Optional[Mapping[str, Any]] = None,
        chunk_size: Optional[int] = None,
        buffer_size: int = 0,
    ):
        self.__pool = HTTPConnectionPool(host, port, maxsize=max_connections, block=block_connections)
        self.__executor = ThreadPoolExecutor()
        self.__metrics = metrics

        self.__options = options if options is not None else {}
        self.__user = user
        self.__password = password
        self.__encoding = encoding
        self.__statement = statement
        self.__buffer_size = buffer_size
        self.__chunk_size = chunk_size
        self.__debug_buffer_size_bytes = state.get_config(
            "debug_buffer_size_bytes", None
        )
        assert (
            isinstance(self.__debug_buffer_size_bytes, int)
            or self.__debug_buffer_size_bytes is None
        )

    def __repr__(self) -> str:
        return f"<{type(self).__name__}: {self.__statement.get_qualified_table()} on {self.__pool.host}:{self.__pool.port}>"

    def write(self, values: Iterable[bytes]) -> None:
        """
        This method is called during both normal operation of a consumer as well as during a
        rebalance when the consumer is shutting down. When calling the join method on the batch,
        we provide a timeout of 10 seconds. If the batch is not finished within that time,
        the exception would be raised and the consumer would be shutdown. Raising an exception is
        much better than waiting forever for the batch to finish and never shutting down the
        consumer. That would cause the consumer to be stuck.
        """
        batch = HTTPWriteBatch(
            self.__executor,
            self.__pool,
            self.__metrics,
            self.__user,
            self.__password,
            self.__statement,
            self.__encoding,
            self.__options,
            self.__chunk_size,
            self.__buffer_size,
            self.__debug_buffer_size_bytes,
        )

        for value in values:
            batch.append(value)

        batch.close()
        batch_join_timeout = state.get_config("http_batch_join_timeout", 10)
        # IMPORTANT: Please read the docstring of this method if you ever decide to remove the
        # timeout argument from the join method.
        batch.join(timeout=batch_join_timeout)
