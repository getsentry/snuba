import pytest

from snuba.clickhouse.native import ClickhouseNativePool, ClickhouseReader
from snuba.web.db_query import _get_cache_partition


@pytest.mark.redis_db
def test_cache_partition() -> None:
    pool = ClickhouseNativePool("127.0.0.1", 9000, "", "", "")
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
