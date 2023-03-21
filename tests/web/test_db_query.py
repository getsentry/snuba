from typing import Any, Mapping, MutableMapping, Optional, Sequence

import pytest

from snuba import state
from snuba.clickhouse.formatter.query import format_query
from snuba.query.query_settings import HTTPQuerySettings
from snuba.state.quota import ResourceQuota
from snuba.state.rate_limit import RateLimitParameters, RateLimitStats
from snuba.web.db_query import (
    _apply_thread_quota_to_clickhouse_query_settings,
    _get_query_settings_from_config,
    db_query,
)

test_data = [
    pytest.param(
        {
            "query_settings/max_threads": 10,
            "query_settings/merge_tree_max_rows_to_use_cache": 50000,
            "some-query-prefix/query_settings/max_threads": 5,
            "some-query-prefix/query_settings/merge_tree_max_rows_to_use_cache": 100000,
        },
        {
            "max_threads": 10,
            "merge_tree_max_rows_to_use_cache": 50000,
        },
        None,
        id="no override when query settings prefix empty",
    ),
    pytest.param(
        {
            "query_settings/max_threads": 10,
            "query_settings/merge_tree_max_rows_to_use_cache": 50000,
            "some-query-prefix/query_settings/max_threads": 5,
            "some-query-prefix/query_settings/merge_tree_max_rows_to_use_cache": 100000,
        },
        {
            "max_threads": 10,
            "merge_tree_max_rows_to_use_cache": 50000,
        },
        "other-query-prefix",
        id="no override for different query prefix",
    ),
    pytest.param(
        {
            "query_settings/max_threads": 10,
            "query_settings/merge_tree_max_rows_to_use_cache": 50000,
            "some-query-prefix/query_settings/max_threads": 5,
            "some-query-prefix/query_settings/merge_tree_max_rows_to_use_cache": 100000,
        },
        {
            "max_threads": 5,
            "merge_tree_max_rows_to_use_cache": 100000,
        },
        "some-query-prefix",
        id="override for same query prefix",
    ),
]


@pytest.mark.parametrize("query_config,expected,query_prefix", test_data)
def test_query_settings_from_config(
    query_config: Mapping[str, Any],
    expected: MutableMapping[str, Any],
    query_prefix: Optional[str],
) -> None:
    for k, v in query_config.items():
        state.set_config(k, v)
    assert _get_query_settings_from_config(query_prefix) == expected


test_thread_quota_data = [
    pytest.param(
        [],
        ResourceQuota(max_threads=5),
        RateLimitStats(rate=1, concurrent=1),
        {"max_threads": 5},
        id="only thread quota",
    )
]


@pytest.mark.parametrize(
    "rate_limit_params,resource_quota,rate_limit_stats,expected_query_settings",
    test_thread_quota_data,
)
def test_apply_thread_quota(
    rate_limit_params: Sequence[RateLimitParameters],
    resource_quota: ResourceQuota,
    rate_limit_stats: RateLimitStats,
    expected_query_settings: Mapping[str, Any],
) -> None:
    settings = HTTPQuerySettings()
    for rlimit in rate_limit_params:
        settings.add_rate_limit(rlimit)
    settings.set_resource_quota(resource_quota)
    clickhouse_query_settings: MutableMapping[str, Any] = {}
    _apply_thread_quota_to_clickhouse_query_settings(
        settings, clickhouse_query_settings, rate_limit_stats
    )
    assert clickhouse_query_settings == expected_query_settings


def test_db_query(ch_query):
    from snuba.clickhouse.native import NativeDriverReader
    from snuba.datasets.storages.factory import get_storage
    from snuba.datasets.storages.storage_key import StorageKey
    from snuba.querylog.query_metadata import SnubaQueryMetadata
    from snuba.utils.metrics.timer import Timer

    reader = get_storage(StorageKey("errors")).get_cluster().get_reader()

    # result = db_query(
    #     clickhouse_query=ch_query,
    #     query_settings=HTTPQuerySettings()
    #     formatted_query=format_query(ch_query),
    #     reader=reader,
    #     timer=Timer(),
    #     query_metadata: SnubaQueryMetadata,
    #     stats: MutableMapping[str, Any],
    #     trace_id: Optional[str] = None,
    #     robust: bool = False,
    # )
