from snuba.web.db_query import _get_cache_partition


def test_cache_partition() -> None:
    default_cache = _get_cache_partition(None)
    another_default_cache = _get_cache_partition(None)

    assert id(default_cache) == id(another_default_cache)

    nondefault_cache = _get_cache_partition("non_default")
    another_nondefault_cache = _get_cache_partition("non_default")

    assert id(nondefault_cache) == id(another_nondefault_cache)
    assert id(default_cache) != id(nondefault_cache)
