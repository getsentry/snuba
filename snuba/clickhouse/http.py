import json
import re
from datetime import datetime
from urllib.parse import urlencode
from typing import Iterable

from urllib3.connectionpool import HTTPConnectionPool
from urllib3.exceptions import HTTPError

from snuba.clickhouse import DATETIME_FORMAT
from snuba.writer import BatchWriter, WriterTableRow


class ClickHouseError(Exception):
    def __init__(self, code: int, type: str, message: str):
        self.code = code
        self.type = type
        self.message = message

    def __str__(self) -> str:
        return f"[{self.code}] {self.type}: {self.message}"

    def __repr__(self) -> str:
        return f"<{type(self).__name__}: {self}>"


CLICKHOUSE_ERROR_RE = re.compile(
    r"^Code: (?P<code>\d+), e.displayText\(\) = (?P<type>(?:\w+)::(?:\w+)): (?P<message>.+)$",
    re.MULTILINE,
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
            content = response.data.decode("utf8")
            details = CLICKHOUSE_ERROR_RE.match(content)
            if details is not None:
                code, type, message = details.groups()
                raise ClickHouseError(int(code), type, message)
            else:
                raise HTTPError(
                    f"Received unexpected {response.status} response: {content}"
                )
