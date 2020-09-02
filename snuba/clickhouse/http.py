import functools
import os
import re
from datetime import datetime
from typing import Any, Mapping, Optional
from urllib.parse import urlencode

import rapidjson
from urllib3.connectionpool import HTTPConnectionPool
from urllib3.exceptions import HTTPError

from snuba.clickhouse import DATETIME_FORMAT
from snuba.clickhouse.errors import ClickhouseError
from snuba.utils.codecs import Encoder
from snuba.utils.concurrent import execute
from snuba.writer import WriteBatch, Writer, WriterTableRow


CLICKHOUSE_ERROR_RE = re.compile(
    r"^Code: (?P<code>\d+), e.displayText\(\) = (?P<type>(?:\w+)::(?:\w+)): (?P<message>.+)$",
    re.MULTILINE,
)


def check_clickhouse_response(response) -> None:
    if response.status != 200:
        # XXX: This should be switched to just parse the JSON body after
        # https://github.com/yandex/ClickHouse/issues/6272 is available.
        content = response.data.decode("utf8")
        details = CLICKHOUSE_ERROR_RE.match(content)
        if details is not None:
            code, type, message = details.groups()
            raise ClickhouseError(int(code), message)
        else:
            raise HTTPError(
                f"Received unexpected {response.status} response: {content}"
            )


JSONRow = bytes  # a single row in JSONEachRow format


class HTTPWriter(Writer[JSONRow]):
    def __init__(
        self,
        host: str,
        port: int,
        database: str,
        table_name: str,
        user: str,
        password: str,
        options: Optional[Mapping[str, Any]] = None,
    ) -> None:
        self.__pool = HTTPConnectionPool(host, port)
        self.__database = database
        self.__table_name = table_name
        self.__user = user
        self.__password = password
        self.__options = options

    def batch(self) -> WriteBatch[JSONRow]:
        return HTTPWriteBatch(
            self.__pool,
            self.__database,
            self.__table_name,
            self.__user,
            self.__password,
            self.__options,
        )


class HTTPWriteBatch(WriteBatch[JSONRow]):
    def __init__(
        self,
        pool: HTTPConnectionPool,
        database: str,
        table_name: str,
        user: str,
        password: str,
        options: Optional[Mapping[str, Any]] = None,
    ) -> None:
        self.__pool = pool

        read_fd, write_fd = os.pipe()
        self.__read = open(read_fd, "rb")
        self.__write = open(write_fd, "wb")

        self.__result = execute(
            functools.partial(
                self.__pool.urlopen,
                "POST",
                "/?"
                + urlencode(
                    {
                        **(options if options is not None else {}),
                        "query": f"INSERT INTO {database}.{table_name} FORMAT JSONEachRow",
                    }
                ),
                headers={
                    "X-ClickHouse-User": user,
                    "X-ClickHouse-Key": password,
                    "Connection": "keep-alive",
                    "Accept-Encoding": "gzip,deflate",
                },
                body=self.__read,
                chunked=True,
            )
        )

    def append(self, value: JSONRow) -> None:
        self.__write.write(value)
        # TODO: Write newlines after each chunk.

    def close(self) -> None:
        self.__write.close()

    def join(self, timeout: Optional[float] = None) -> None:
        check_clickhouse_response(self.__result.result(timeout))


class JSONRowEncoder(Encoder[JSONRow, WriterTableRow]):
    def __default(self, value: Any) -> Any:
        if isinstance(value, datetime):
            return value.strftime(DATETIME_FORMAT)
        else:
            raise TypeError

    def encode(self, value: WriterTableRow) -> JSONRow:
        return rapidjson.dumps(value, default=self.__default).encode("utf-8")
