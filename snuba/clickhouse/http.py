import json
import re
from typing import Callable, Iterable, Mapping, MutableMapping, Optional
from urllib.parse import urlencode

from dateutil.parser import parse as dateutil_parse
from urllib3.connectionpool import HTTPConnectionPool
from urllib3.exceptions import HTTPError
from urllib3.response import HTTPResponse

from snuba.clickhouse.errors import ClickhouseError
from snuba.clickhouse.native import transform_date, transform_datetime
from snuba.clickhouse.query import ClickhouseQuery
from snuba.datasets.schemas.tables import TableSchema
from snuba.reader import Reader, Result, build_result_transformer
from snuba.writer import BatchWriter, WriterTableRow


CLICKHOUSE_ERROR_RE = re.compile(
    r"^Code: (?P<code>\d+), e.displayText\(\) = (?P<type>(?:\w+)::(?:\w+)): (?P<message>.+)$",
    re.MULTILINE,
)


def raise_for_error_response(response: HTTPResponse) -> None:
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

        raise_for_error_response(response)


def parse_and_transform_date(value: str) -> str:
    return transform_date(dateutil_parse(value))


def parse_and_transform_datetime(value: str) -> str:
    return transform_datetime(dateutil_parse(value))


transform_column_types = build_result_transformer(
    [
        (re.compile(r"^Date(\(.+\))?$"), parse_and_transform_date),
        (re.compile(r"^DateTime(\(.+\))?$"), parse_and_transform_datetime),
    ]
)


class HTTPReader(Reader[ClickhouseQuery]):
    def __init__(
        self, host: str, port: int, settings: Optional[Mapping[str, str]] = None
    ):
        if settings is not None:
            assert "query_id" not in settings, "query_id cannot be passed as a setting"

        self.__pool = HTTPConnectionPool(host, port)

        self.__default_settings: MutableMapping[str, str] = (
            {**settings} if settings is not None else {}
        )

        if "output_format_json_quote_64bit_integers" not in self.__default_settings:
            self.__default_settings["output_format_json_quote_64bit_integers"] = "0"

    def execute(
        self,
        query: ClickhouseQuery,
        settings: Optional[Mapping[str, str]] = None,
        query_id: Optional[str] = None,
        with_totals: bool = False,  # NOTE: unnecessary with FORMAT JSON
    ) -> Result:
        query_settings: MutableMapping[str, str] = (
            {**settings} if settings is not None else {}
        )

        # XXX: mypy won't allow redefining ``settings`` as mutable, so delete
        # the original variable to avoid accidentally referencing ``settings``
        # instead of ``query_settings``.
        del settings

        assert (
            "query_id" not in query_settings
        ), "query_id cannot be passed as a setting"

        if query_id is not None:
            query_settings["query_id"] = query_id

        response = self.__pool.urlopen(
            "POST",
            "/?" + urlencode({**self.__default_settings, **query_settings}),
            headers={"Connection": "keep-alive", "Accept-Encoding": "gzip,deflate"},
            body=query.format_sql("JSON"),
        )

        raise_for_error_response(response)

        result = json.loads(response.data.decode("utf-8"))

        # Remove any extra keys that are not part of the Result data structure.
        for k in [*result.keys()]:
            if k not in {"meta", "data", "totals"}:
                del result[k]

        transform_column_types(result)

        return result
