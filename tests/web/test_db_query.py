from __future__ import annotations

from typing import Any, Mapping, MutableMapping, Optional, Sequence

import pytest

from snuba import state
from snuba.attribution.appid import AppID
from snuba.attribution.attribution_info import AttributionInfo
from snuba.clickhouse.formatter.query import format_query
from snuba.clickhouse.query import Query as ClickhouseQuery
from snuba.datasets.storages.factory import get_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.query import SelectedExpression
from snuba.query.data_source.simple import Table
from snuba.query.parser.expressions import parse_clickhouse_function
from snuba.query.query_settings import HTTPQuerySettings
from snuba.querylog.query_metadata import ClickhouseQueryMetadata
from snuba.state.quota import ResourceQuota
from snuba.state.rate_limit import RateLimitParameters, RateLimitStats
from snuba.utils.metrics.timer import Timer
from snuba.web import QueryException
from snuba.web.db_query import (
    _apply_thread_quota_to_clickhouse_query_settings,
    _get_query_settings_from_config,
    raw_query,
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
    clickhouse_query_settings: dict[str, Any] = {}
    _apply_thread_quota_to_clickhouse_query_settings(
        settings, clickhouse_query_settings, rate_limit_stats
    )
    assert clickhouse_query_settings == expected_query_settings


def test_raw_query_success() -> None:
    storage = get_storage(StorageKey("errors"))
    query = ClickhouseQuery(
        from_clause=Table(
            storage.get_schema().get_data_source().get_table_name(),  # type: ignore
            schema=storage.get_schema().get_columns(),
            final=False,
        ),
        selected_columns=[
            SelectedExpression(
                "porject_count",
                parse_clickhouse_function("count(distinct(project_id))"),
            )
        ],
    )

    query_metadata_list: list[ClickhouseQueryMetadata] = []
    stats: dict[str, Any] = {}

    result = raw_query(
        clickhouse_query=query,
        query_settings=HTTPQuerySettings(),
        attribution_info=AttributionInfo(
            app_id=AppID(key="key"), tenant_ids={}, referrer="something"
        ),
        dataset_name="events",
        query_metadata_list=query_metadata_list,
        formatted_query=format_query(query),
        reader=storage.get_cluster().get_reader(),
        timer=Timer("foo"),
        stats=stats,
        trace_id="trace_id",
        robust=False,
    )
    assert len(query_metadata_list) == 1
    assert result.extra["stats"] == stats
    assert result.extra["sql"] is not None
    assert set(result.result["profile"].keys()) == {  # type: ignore
        "elapsed",
        "bytes",
        "blocks",
        "rows",
    }


def test_raw_query_fail() -> None:
    storage = get_storage(StorageKey("errors"))
    query = ClickhouseQuery(
        from_clause=Table(
            storage.get_schema().get_data_source().get_table_name(),  # type: ignore
            schema=storage.get_schema().get_columns(),
            final=False,
        ),
        selected_columns=[
            SelectedExpression(
                "bad_column", parse_clickhouse_function("count(non_existent_column)")
            )
        ],
    )

    query_metadata_list: list[ClickhouseQueryMetadata] = []
    stats: dict[str, Any] = {}
    exception_raised = False
    try:
        raw_query(
            clickhouse_query=query,
            query_settings=HTTPQuerySettings(),
            attribution_info=AttributionInfo(
                app_id=AppID(key="key"), tenant_ids={}, referrer="something"
            ),
            dataset_name="events",
            query_metadata_list=query_metadata_list,
            formatted_query=format_query(query),
            reader=storage.get_cluster().get_reader(),
            timer=Timer("foo"),
            stats=stats,
            trace_id="trace_id",
            robust=False,
        )
    except QueryException as e:
        exception_raised = True
        assert len(query_metadata_list) == 1
        assert query_metadata_list[0].status.value == "error"
        assert e.extra["stats"] == stats

        assert e.extra["sql"] is not None
    assert exception_raised
