from snuba.clickhouse.native import ClickhousePool, NativeDriverReader
from snuba.web.db_query import _get_cache_partition


def test_cache_partition() -> None:
    pool = ClickhousePool("localhost", 9000, "", "", "")
    reader1 = NativeDriverReader(None, pool)
    reader2 = NativeDriverReader(None, pool)

    default_cache = _get_cache_partition(reader1)
    another_default_cache = _get_cache_partition(reader2)

    assert id(default_cache) == id(another_default_cache)

    reader3 = NativeDriverReader("non_default", pool)
    reader4 = NativeDriverReader("non_default", pool)
    nondefault_cache = _get_cache_partition(reader3)
    another_nondefault_cache = _get_cache_partition(reader4)

    assert id(nondefault_cache) == id(another_nondefault_cache)
    assert id(default_cache) != id(nondefault_cache)
