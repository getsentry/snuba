from collections import Mapping
from typing import Any, MutableMapping, Optional

import pytest

from snuba import state
from snuba.web.db_query import _get_query_settings_from_config

test_data = [
    pytest.param(
        {
            "query_settings/max_threads": 10,
            "query_settings/merge_tree_max_rows_to_use_cache": 50000,
            "some-cache-partition/query_settings/max_threads": 5,
        },
        {
            "max_threads": 10,
            "merge_tree_max_rows_to_use_cache": 50000,
        },
        None,
        id="no override when cache partition empty",
    ),
    pytest.param(
        {
            "query_settings/max_threads": 10,
            "query_settings/merge_tree_max_rows_to_use_cache": 50000,
            "some-cache-partition/query_settings/max_threads": 5,
        },
        {
            "max_threads": 10,
            "merge_tree_max_rows_to_use_cache": 50000,
        },
        "other-cache-partition",
        id="no override for different cache partition",
    ),
    pytest.param(
        {
            "query_settings/max_threads": 10,
            "query_settings/merge_tree_max_rows_to_use_cache": 50000,
            "some-cache-partition/query_settings/max_threads": 5,
            "some-cache-partition/query_settings/merge_tree_max_rows_to_use_cache": 100000,
        },
        {
            "max_threads": 5,
            "merge_tree_max_rows_to_use_cache": 100000,
        },
        "some-cache-partition",
        id="override for same cache partition",
    ),
]


@pytest.mark.parametrize("query_config,expected,cache_partition", test_data)
def test_query_settings_from_config(
    query_config: Mapping[str, Any],
    expected: MutableMapping[str, Any],
    cache_partition: Optional[str],
) -> None:
    for k, v in query_config.items():
        state.set_config(k, v)
    assert _get_query_settings_from_config(cache_partition) == expected
