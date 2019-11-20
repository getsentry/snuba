import re
from urllib.parse import urlencode
from typing import Callable, Iterable

from urllib3.connectionpool import HTTPConnectionPool
from urllib3.exceptions import HTTPError

from snuba.datasets.schemas.tables import TableSchema
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
    def __init__(
        self,
        schema: TableSchema,
        host,
        port,
        encoder: Callable[[WriterTableRow], bytes],
        options=None,
        table_name=None,
        chunk_size: int = 1,
    ):
        """
        Builds a writer to send a batch to Clickhouse.

        :param schema: The dataset schema to take the table name from
        :param host: Clickhosue host
        :param port: Clickhosue port
        :param encoder: A function that will be applied to each row to turn it into bytes
        :param options: options passed to Clickhouse
        :param table_name: Overrides the table coming from the schema (generally used for uplaoding
            on temporary tables)
        :param chunk_size: The chunk size (in rows).
            We send data to the server with Transfer-Encoding: chunked. If 0 we send the entire
            content in one chunk.
        """
        self.__pool = HTTPConnectionPool(host, port)
        self.__options = options if options is not None else {}
        self.__table_name = table_name or schema.get_table_name()
        self.__chunk_size = chunk_size
        self.__encoder = encoder

    def _prepare_chunks(self, rows: Iterable[WriterTableRow]) -> Iterable[bytes]:
        chunk = []
        for row in rows:
            chunk.append(self.__encoder(row))
            if self.__chunk_size and len(chunk) == self.__chunk_size:
                yield b"".join(chunk)
                chunk = []

        if chunk:
            yield b"".join(chunk)

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
            body=self._prepare_chunks(rows),
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
