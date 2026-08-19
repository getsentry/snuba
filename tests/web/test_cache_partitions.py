from collections.abc import Mapping, Sequence
from typing import Any

import pytest

from snuba.clickhouse.pool import ClickhousePool, ClickhouseResult, Params
from snuba.clickhouse.reader import ClickhouseReader
from snuba.web.db_query import _get_cache_partition


class _StubPool(ClickhousePool):
    def __init__(self) -> None:
        self.host = "127.0.0.1"
        self.port = 8123
        self.user = ""
        self.password = ""
        self.database = ""

    def execute(
        self,
        query: str,
        params: Params = None,
        with_column_types: bool = False,
        query_id: str | None = None,
        settings: Mapping[str, Any] | None = None,
        types_check: bool = False,
        columnar: bool = False,
        capture_trace: bool = False,
    ) -> ClickhouseResult:
        return ClickhouseResult()

    def execute_robust(
        self,
        query: str,
        params: Params = None,
        with_column_types: bool = False,
        query_id: str | None = None,
        settings: Mapping[str, Any] | None = None,
        types_check: bool = False,
        columnar: bool = False,
        capture_trace: bool = False,
    ) -> ClickhouseResult:
        return self.execute(query)

    def command(
        self,
        statement: str,
        params: Params = None,
        settings: Mapping[str, Any] | None = None,
        query_id: str | None = None,
    ) -> ClickhouseResult:
        return ClickhouseResult()

    def execute_explain(
        self, query: str, settings: Mapping[str, Any] | None = None
    ) -> ClickhouseResult:
        return self.execute(query, settings=settings)

    def execute_with_totals(
        self,
        query: str,
        params: Params = None,
        query_id: str | None = None,
        settings: Mapping[str, Any] | None = None,
        capture_trace: bool = False,
        robust: bool = False,
    ) -> ClickhouseResult:
        return self.execute(query)

    def insert(
        self,
        table: str,
        data: Sequence[Mapping[str, Any]],
        settings: Mapping[str, Any] | None = None,
        query_id: str | None = None,
    ) -> None:
        return None

    def close(self) -> None:
        return None


@pytest.mark.redis_db
def test_cache_partition() -> None:
    pool = _StubPool()
    reader1 = ClickhouseReader(None, pool, None)
    reader2 = ClickhouseReader(None, pool, None)

    default_cache = _get_cache_partition(reader1)
    another_default_cache = _get_cache_partition(reader2)

    assert id(default_cache) == id(another_default_cache)

    reader3 = ClickhouseReader("non_default", pool, None)
    reader4 = ClickhouseReader("non_default", pool, None)
    nondefault_cache = _get_cache_partition(reader3)
    another_nondefault_cache = _get_cache_partition(reader4)

    assert id(nondefault_cache) == id(another_nondefault_cache)
    assert id(default_cache) != id(nondefault_cache)
