import pytest

from snuba.clickhouse.native import ClickhousePool, ClickhouseReader, ClickhouseResult, Params
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
        settings=None,
        types_check: bool = False,
        columnar: bool = False,
        capture_trace: bool = False,
        retryable: bool = True,
    ) -> ClickhouseResult:
        return ClickhouseResult()

    def execute_robust(
        self,
        query: str,
        params: Params = None,
        with_column_types: bool = False,
        query_id: str | None = None,
        settings=None,
        types_check: bool = False,
        columnar: bool = False,
        capture_trace: bool = False,
        retryable: bool = True,
    ) -> ClickhouseResult:
        return self.execute(query)

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
