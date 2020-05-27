import re
from urllib.parse import urlencode
from typing import Any, Callable, Iterable, Mapping, Optional

from urllib3.connectionpool import HTTPConnectionPool
from urllib3.exceptions import HTTPError

from snuba.clickhouse.errors import ClickhouseError
from snuba.writer import BatchWriter, WriterTableRow


CLICKHOUSE_ERROR_RE = re.compile(
    r"^Code: (?P<code>\d+), e.displayText\(\) = (?P<type>(?:\w+)::(?:\w+)): (?P<message>.+)$",
    re.MULTILINE,
)


class HTTPBatchWriter(BatchWriter):
    def __init__(
        self,
        table_name: str,
        host: str,
        port: int,
        user: str,
        password: str,
        encoder: Callable[[WriterTableRow], bytes],
        options: Optional[Mapping[str, Any]] = None,
        chunk_size: Optional[int] = 1,
    ):
        """
        Builds a writer to send a batch to Clickhouse.
        The encoder function will be applied to each row to turn it into bytes.
        We send data to the server with Transfer-Encoding: chunked. If chunk size is 0
        we send the entire content in one chunk, otherwise it is the rows per chunk.
        """
        self.__pool = HTTPConnectionPool(host, port)
        self.__options = options if options is not None else {}
        self.__table_name = table_name
        self.__chunk_size = chunk_size
        self.__encoder = encoder
        self.__user = user
        self.__password = password

    def _prepare_chunks(self, rows: Iterable[WriterTableRow]) -> Iterable[bytes]:
        chunk = []
        for row in rows:
            chunk.append(self.__encoder(row))
            if self.__chunk_size and len(chunk) == self.__chunk_size:
                yield b"".join(chunk)
                chunk = []

        if chunk:
            yield b"".join(chunk)

    def write(self, rows: Iterable[WriterTableRow]) -> None:
        response = self.__pool.urlopen(
            "POST",
            "/?"
            + urlencode(
                {
                    **self.__options,
                    "query": f"INSERT INTO {self.__table_name} FORMAT JSONEachRow",
                }
            ),
            headers={
                "X-ClickHouse-User": self.__user,
                "X-ClickHouse-Key": self.__password,
                "Connection": "keep-alive",
                "Accept-Encoding": "gzip,deflate"
            },
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
                raise ClickhouseError(int(code), message)
            else:
                raise HTTPError(
                    f"Received unexpected {response.status} response: {content}"
                )
