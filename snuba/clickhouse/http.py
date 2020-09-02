import os
import re
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import Any, Iterable, Mapping, Optional
from urllib.parse import urlencode

import rapidjson
from urllib3.connectionpool import HTTPConnectionPool
from urllib3.exceptions import HTTPError

from snuba.clickhouse import DATETIME_FORMAT
from snuba.clickhouse.errors import ClickhouseWriterError
from snuba.utils.codecs import Encoder
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
    ) -> None:
        read_fd, write_fd = os.pipe()
        self.__input = open(write_fd, "wb")

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
            body=open(read_fd, "rb"),
        )

    def append(self, value: JSONRow) -> None:
        self.__input.write(value)

    def close(self) -> None:
        self.__input.close()

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
        chunk_size: Optional[int] = None,  # deprecated
    ):
        self.__pool = HTTPConnectionPool(host, port)
        self.__executor = ThreadPoolExecutor()

        self.__options = options if options is not None else {}
        self.__table_name = table_name
        self.__user = user
        self.__password = password
        self.__database = database

    def write(self, values: Iterable[JSONRow]) -> None:
        batch = HTTPWriteBatch(
            self.__executor,
            self.__pool,
            self.__database,
            self.__table_name,
            self.__user,
            self.__password,
            self.__options,
        )

        for value in values:
            batch.append(value)

        batch.close()
        batch.join()
