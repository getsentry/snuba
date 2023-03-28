from __future__ import annotations

from typing import Any, Mapping, MutableMapping, Optional, Sequence
from unittest import mock

import pytest

from snuba import state
from snuba.attribution.appid import AppID
from snuba.attribution.attribution_info import AttributionInfo
from snuba.clickhouse.formatter.query import format_query
from snuba.clickhouse.query import Query as ClickhouseQuery
from snuba.datasets.storage import Storage
from snuba.datasets.storages.factory import get_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.query import SelectedExpression
from snuba.query.allocation_policies import (
    AllocationPolicy,
    AllocationPolicyViolation,
    QueryResultOrError,
    QuotaAllowance,
)
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
@pytest.mark.redis_db
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


def _build_test_query(select_expression: str) -> tuple[ClickhouseQuery, Storage]:
    storage = get_storage(StorageKey("errors"))
    return (
        ClickhouseQuery(
            from_clause=Table(
                storage.get_schema().get_data_source().get_table_name(),  # type: ignore
                schema=storage.get_schema().get_columns(),
                final=False,
            ),
            selected_columns=[
                SelectedExpression(
                    "some_alias",
                    parse_clickhouse_function(select_expression),
                )
            ],
        ),
        storage,
    )


@pytest.mark.clickhouse_db
@pytest.mark.redis_db
def test_db_query_success() -> None:
    query, storage = _build_test_query("count(distinct(project_id))")

    query_metadata_list: list[ClickhouseQueryMetadata] = []
    stats: dict[str, Any] = {}

    result = db_query(
        clickhouse_query=query,
        query_settings=HTTPQuerySettings(),
        attribution_info=AttributionInfo(
            app_id=AppID(key="key"),
            tenant_ids={},
            referrer="something",
            team=None,
            feature=None,
            parent_api=None,
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


@pytest.mark.clickhouse_db
@pytest.mark.redis_db
def test_db_query_fail() -> None:
    query, storage = _build_test_query("count(non_existent_column)")

    query_metadata_list: list[ClickhouseQueryMetadata] = []
    stats: dict[str, Any] = {}
    exception_raised = False
    try:
        db_query(
            clickhouse_query=query,
            query_settings=HTTPQuerySettings(),
            attribution_info=AttributionInfo(
                app_id=AppID(key="key"),
                tenant_ids={},
                referrer="something",
                team=None,
                feature=None,
                parent_api=None,
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


def test_db_query_with_rejecting_allocation_policy() -> None:
    # this test does not need the db or a query because the allocation policy
    # should reject the query before it gets to execution
    class RejectAllocationPolicy(AllocationPolicy):
        def _get_quota_allowance(
            self, tenant_ids: dict[str, str | int]
        ) -> QuotaAllowance:
            return QuotaAllowance(
                can_run=False,
                max_threads=0,
                explanation={"reason": "policy rejects all queries"},
            )

        def _update_quota_balance(
            self,
            tenant_ids: dict[str, str | int],
            result_or_error: QueryResultOrError,
        ) -> None:
            return

    with mock.patch(
        "snuba.web.db_query._get_allocation_policy",
        return_value=RejectAllocationPolicy("doesntmatter", ["a", "b", "c"]),  # type: ignore
    ):
        query_metadata_list: list[ClickhouseQueryMetadata] = []
        stats: dict[str, Any] = {}
        exception_raised = False
        try:
            db_query(
                clickhouse_query=mock.Mock(),
                query_settings=HTTPQuerySettings(),
                attribution_info=AttributionInfo(
                    app_id=AppID(key="key"),
                    tenant_ids={},
                    referrer="something",
                    team=None,
                    feature=None,
                    parent_api=None,
                ),
                dataset_name="events",
                query_metadata_list=query_metadata_list,
                formatted_query=mock.Mock(),
                reader=mock.Mock(),
                timer=Timer("foo"),
                stats=stats,
                trace_id="trace_id",
                robust=False,
            )
        except QueryException as e:
            exception_raised = True
            cause = e.__cause__
            assert isinstance(cause, AllocationPolicyViolation)
            assert cause.extra_data["quota_allowance"]["explanation"]["reason"] == "policy rejects all queries"  # type: ignore

        assert exception_raised


@pytest.mark.clickhouse_db
@pytest.mark.redis_db
def test_allocation_policy_threads_applied_to_query() -> None:
    POLICY_THREADS = 4

    class ThreadLimitPolicy(AllocationPolicy):
        def _get_quota_allowance(
            self, tenant_ids: dict[str, str | int]
        ) -> QuotaAllowance:
            return QuotaAllowance(
                can_run=True,
                max_threads=POLICY_THREADS,
                explanation={"reason": "Throttle everything!"},
            )

        def _update_quota_balance(
            self,
            tenant_ids: dict[str, str | int],
            result_or_error: QueryResultOrError,
        ) -> None:
            return

    with mock.patch(
        "snuba.web.db_query._get_allocation_policy",
        return_value=ThreadLimitPolicy("doesntmatter", ["a", "b", "c"]),  # type: ignore
    ):
        query, storage = _build_test_query("count(distinct(project_id))")

        query_metadata_list: list[ClickhouseQueryMetadata] = []
        stats: dict[str, Any] = {}
        settings = HTTPQuerySettings()
        db_query(
            clickhouse_query=query,
            query_settings=settings,
            attribution_info=AttributionInfo(
                app_id=AppID(key="key"),
                tenant_ids={},
                referrer="something",
                team=None,
                feature=None,
                parent_api=None,
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
        assert settings.get_resource_quota().max_threads == POLICY_THREADS
        assert stats["max_threads"] == POLICY_THREADS
        assert query_metadata_list[0].stats["max_threads"] == POLICY_THREADS
