import pytest

from snuba import state
from snuba.clickhouse.native import ClickhousePool, NativeDriverReader
from snuba.web.db_query import _get_cache_partition, _get_cache_wait_timeout


@pytest.mark.redis_db
def test_cache_partition() -> None:
    pool = ClickhousePool("127.0.0.1", 9000, "", "", "")
    reader1 = NativeDriverReader(None, pool, None)
    reader2 = NativeDriverReader(None, pool, None)

    default_cache = _get_cache_partition(reader1)
    another_default_cache = _get_cache_partition(reader2)

    assert id(default_cache) == id(another_default_cache)

    reader3 = NativeDriverReader("non_default", pool, None)
    reader4 = NativeDriverReader("non_default", pool, None)
    nondefault_cache = _get_cache_partition(reader3)
    another_nondefault_cache = _get_cache_partition(reader4)

    assert id(nondefault_cache) == id(another_nondefault_cache)
    assert id(default_cache) != id(nondefault_cache)


@pytest.mark.redis_db
def test_cache_wait_timeout() -> None:
    pool = ClickhousePool("127.0.0.1", 9000, "", "", "")
    default_reader = NativeDriverReader(None, pool, None)
    tiger_errors_reader = NativeDriverReader("tiger_errors", pool, None)
    tiger_transactions_reader = NativeDriverReader("tiger_transactions", pool, None)

    query_settings = {"max_execution_time": 30}
    assert _get_cache_wait_timeout(query_settings, default_reader) == 30
    assert _get_cache_wait_timeout(query_settings, tiger_errors_reader) == 30
    assert _get_cache_wait_timeout(query_settings, tiger_transactions_reader) == 30

    state.set_config("tiger-cache-wait-time", 60)
    assert _get_cache_wait_timeout(query_settings, default_reader) == 30
    assert _get_cache_wait_timeout(query_settings, tiger_errors_reader) == 60
    assert _get_cache_wait_timeout(query_settings, tiger_transactions_reader) == 60

    state.delete_config("tiger-cache-wait-time")
