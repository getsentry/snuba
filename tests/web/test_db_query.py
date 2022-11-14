from typing import Any, Mapping, MutableMapping, Optional, Sequence

import pytest

from snuba import state
from snuba.query.query_settings import HTTPQuerySettings
from snuba.state.quota import ResourceQuota
from snuba.state.rate_limit import RateLimitParameters, RateLimitStats
from snuba.web.db_query import (
    _apply_thread_quota_to_clickhouse_query_settings,
    _get_query_settings_from_config,
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
    expected_query_settings: dict,
) -> None:
    settings = HTTPQuerySettings()
    for rlimit in rate_limit_params:
        settings.add_rate_limit(rlimit)
    settings.set_resource_quota(resource_quota)
    clickhouse_query_settings = _apply_thread_quota_to_clickhouse_query_settings(
        settings, {}, rate_limit_stats
    )
    assert clickhouse_query_settings == expected_query_settings
