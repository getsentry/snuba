from typing import Sequence, Tuple

import pytest

from snuba import settings
from snuba.clickhouse.errors import ClickhouseError
from snuba.clickhouse.http import HTTPReader
from snuba.clickhouse.native import NativeDriverReader
from snuba.clickhouse.query import ClickhouseQuery
from snuba.environment import clickhouse_ro
from snuba.reader import Reader


class SimpleClickhouseQuery(ClickhouseQuery):
    def __init__(self, columns: Sequence[Tuple[str, str]]) -> None:
        self.__columns = columns

    def _format_query_impl(self) -> str:
        columns = ", ".join(f"{value} as {alias}" for alias, value in self.__columns)
        return f"SELECT {columns}"


@pytest.mark.parametrize(
    "reader",
    [
        NativeDriverReader(clickhouse_ro),
        HTTPReader(settings.CLICKHOUSE_HOST, settings.CLICKHOUSE_HTTP_PORT),
    ],
)
def test_reader(reader: Reader[ClickhouseQuery]) -> None:
    assert reader.execute(
        SimpleClickhouseQuery(
            [
                ("datetime", "toDateTime('2020-01-02 03:04:05')"),
                ("date", "toDate('2020-01-02')"),
                ("uuid", "toUUID('00000000-0000-4000-8000-000000000000')"),
            ]
        )
    ) == {
        "meta": [
            {"name": "datetime", "type": "DateTime"},
            {"name": "date", "type": "Date"},
            {"name": "uuid", "type": "UUID"},
        ],
        "data": [
            {
                "date": "2020-01-02T00:00:00+00:00",
                "datetime": "2020-01-02T03:04:05+00:00",
                "uuid": "00000000-0000-4000-8000-000000000000",
            }
        ],
    }

    with pytest.raises(ClickhouseError) as e:
        reader.execute(SimpleClickhouseQuery([("invalid", '"')]))

    assert e.value.code == 62
