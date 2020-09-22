from __future__ import annotations

import re
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from queue import SimpleQueue
from typing import Any, Iterable, Iterator, Mapping, Optional, Union
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


CLICKHOUSE_ERROR_RE = re.compile(
    r"^Code: (?P<code>\d+), e.displayText\(\) = (?P<type>(?:\w+)::(?:\w+)): (?P<message>.+?(?: \(at row (?P<row>\d+)\))?)$",
    re.MULTILINE,
)


JSONRow = bytes  # a single row in JSONEachRow format


class JSONRowEncoder(Encoder[JSONRow, WriterTableRow]):
    def __default(self, value: Any) -> Any:
        if isinstance(value, datetime):
            return value.strftime(DATETIME_FORMAT)
        else:
            raise TypeError

    def encode(self, value: WriterTableRow) -> JSONRow:
        return rapidjson.dumps(value, default=self.__default).encode("utf-8")


class HTTPWriteBatch:
    def __init__(
        self,
        executor: ThreadPoolExecutor,
        pool: HTTPConnectionPool,
        database: str,
        table_name: str,
        user: str,
        password: str,
        options: Mapping[str, Any],  # should be ``Mapping[str, str]``?
        chunk_size: Optional[int] = None,
    ) -> None:
        if chunk_size is None:
            chunk_size = settings.CLICKHOUSE_HTTP_CHUNK_SIZE

        self.__queue: SimpleQueue[Union[JSONRow, None]] = SimpleQueue()

        body = self.__read_until_eof()
        if chunk_size > 1:
            body = (b"".join(chunk) for chunk in chunked(body, chunk_size))
        elif not chunk_size > 0:
            raise ValueError("chunk size must be greater than zero")

        self.__result = executor.submit(
            pool.urlopen,
            "POST",
            "/?"
            + urlencode(
                {
                    **options,
                    "query": f"INSERT INTO {database}.{table_name} FORMAT JSONEachRow",
                }
            ),
            headers={
                "X-ClickHouse-User": user,
                "X-ClickHouse-Key": password,
                "Connection": "keep-alive",
                "Accept-Encoding": "gzip,deflate",
            },
            body=body,
        )

        self.__rows = 0
        self.__size = 0
        self.__closed = False

    def __repr__(self) -> str:
        return f"<{type(self).__name__}: {self.__rows} rows ({self.__size} bytes)>"

    def __read_until_eof(self) -> Iterator[JSONRow]:
        while True:
            value = self.__queue.get()
            if value is None:
                return

            yield value

    def append(self, value: JSONRow) -> None:
        assert not self.__closed

        self.__queue.put(value)
        self.__rows += 1
        self.__size += len(value)

    def close(self) -> None:
        self.__queue.put(None)
        self.__closed = True

    def join(self, timeout: Optional[float] = None) -> None:
        response = self.__result.result(timeout)

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


class HTTPBatchWriter(BatchWriter[JSONRow]):
    def __init__(
        self,
        table_name: str,
        host: str,
        port: int,
        user: str,
        password: str,
        database: str,
        metrics: MetricsBackend,  # deprecated
        options: Optional[Mapping[str, Any]] = None,
        chunk_size: Optional[int] = None,
    ):
        self.__pool = HTTPConnectionPool(host, port)
        self.__executor = ThreadPoolExecutor()

        self.__options = options if options is not None else {}
        self.__table_name = table_name
        self.__user = user
        self.__password = password
        self.__database = database
        self.__chunk_size = chunk_size

    def write(self, values: Iterable[JSONRow]) -> None:
        batch = HTTPWriteBatch(
            self.__executor,
            self.__pool,
            self.__database,
            self.__table_name,
            self.__user,
            self.__password,
            self.__options,
            self.__chunk_size,
        )

        for value in values:
            batch.append(value)

        batch.close()
        batch.join()
