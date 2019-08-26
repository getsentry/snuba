import json
import re
from datetime import datetime
from urllib.parse import urlencode
from typing import Iterable, Mapping, Optional

from urllib3.connectionpool import HTTPConnectionPool
from urllib3.exceptions import HTTPError

from snuba.clickhouse import DATETIME_FORMAT
from snuba.reader import Reader, Result, transform_date_columns
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


def handle_clickhouse_response(response) -> None:
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

        handle_clickhouse_response(response)


class HTTPReader(Reader):
    def __init__(
        self, host: str, port: int, settings: Optional[Mapping[str, str]] = None
    ):
        if settings is not None:
            assert "query_id" not in settings, "query_id cannot be passed as a setting"
        self.__pool = HTTPConnectionPool(host, port)
        self.__default_settings = settings if settings is not None else {}

    def execute(
        self,
        query: str,
        settings: Optional[Mapping[str, str]] = None,
        query_id: Optional[str] = None,
        with_totals: bool = False,  # NOTE: unnecessary with FORMAT JSON
    ) -> Result:
        if settings is None:
            settings = {}

        assert "query_id" not in settings, "query_id cannot be passed as a setting"
        if query_id is not None:
            settings["query_id"] = query_id

        response = self.__pool.urlopen(
            "POST",
            "/?" + urlencode({**self.__default_settings, **settings}),
            headers={"Connection": "keep-alive", "Accept-Encoding": "gzip,deflate"},
            body=query.encode("utf-8"),
        )

        handle_clickhouse_response(response)

        result = json.loads(response.data.decode("utf-8"))
        del result["statistics"]
        del result["rows"]

        return transform_date_columns(result)
