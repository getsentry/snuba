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
    AllocationPolicyConfig,
    AllocationPolicyViolations,
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


def _build_test_query(
    select_expression: str, allocation_policies: list[AllocationPolicy] | None = None
) -> tuple[ClickhouseQuery, Storage, AttributionInfo]:
    storage = get_storage(StorageKey("errors_ro"))
    return (
        ClickhouseQuery(
            from_clause=Table(
                storage.get_schema().get_data_source().get_table_name(),  # type: ignore
                schema=storage.get_schema().get_columns(),
                final=False,
                allocation_policies=allocation_policies
                or storage.get_allocation_policies(),
            ),
            selected_columns=[
                SelectedExpression(
                    "some_alias",
                    parse_clickhouse_function(select_expression),
                )
            ],
        ),
        storage,
        AttributionInfo(
            app_id=AppID(key="key"),
            tenant_ids={"referrer": "something", "organization_id": 1234},
            referrer="something",
            team=None,
            feature=None,
            parent_api=None,
        ),
    )


@pytest.mark.clickhouse_db
@pytest.mark.redis_db
def test_db_query_success() -> None:
    query, storage, attribution_info = _build_test_query("count(distinct(project_id))")

    query_metadata_list: list[ClickhouseQueryMetadata] = []
    stats: dict[str, Any] = {}

    result = db_query(
        clickhouse_query=query,
        query_settings=HTTPQuerySettings(),
        attribution_info=attribution_info,
        dataset_name="events",
        query_metadata_list=query_metadata_list,
        formatted_query=format_query(query),
        reader=storage.get_cluster().get_reader(),
        timer=Timer("foo"),
        stats=stats,
        trace_id="trace_id",
        robust=False,
    )
    assert stats["quota_allowance"] == {
        "BytesScannedWindowAllocationPolicy": {
            "can_run": True,
            "explanation": {},
            "max_threads": 10,
        }
    }
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
def test_db_query_bypass_cache() -> None:
    query, storage, attribution_info = _build_test_query("count(distinct(project_id))")
    state.set_config("bypass_readthrough_cache_probability.errors_local", 0.3)

    query_metadata_list: list[ClickhouseQueryMetadata] = []
    stats: dict[str, Any] = {"clickhouse_table": "errors_local"}

    # cache should not be used for the errors table
    # so if the bypass does not work, the test will try to
    # use a bad cache
    with mock.patch("snuba.web.db_query._get_cache_partition"):
        # random() is less than `bypass_readthrough_cache_probability` therefore we bypass the cache
        with mock.patch("snuba.web.db_query.random", return_value=0.2):
            result = db_query(
                clickhouse_query=query,
                query_settings=HTTPQuerySettings(),
                attribution_info=attribution_info,
                dataset_name="events",
                query_metadata_list=query_metadata_list,
                formatted_query=format_query(query),
                reader=storage.get_cluster().get_reader(),
                timer=Timer("foo"),
                stats=stats,
                trace_id="trace_id",
                robust=False,
            )
            assert stats["quota_allowance"] == {
                "BytesScannedWindowAllocationPolicy": {
                    "can_run": True,
                    "explanation": {},
                    "max_threads": 10,
                }
            }
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
    query, storage, attribution_info = _build_test_query("count(non_existent_column)")

    query_metadata_list: list[ClickhouseQueryMetadata] = []
    stats: dict[str, Any] = {}
    with pytest.raises(QueryException) as excinfo:
        db_query(
            clickhouse_query=query,
            query_settings=HTTPQuerySettings(),
            attribution_info=attribution_info,
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
    assert query_metadata_list[0].request_status.status.value == "error"
    assert excinfo.value.extra["stats"] == stats
    assert excinfo.value.extra["sql"] is not None


def test_db_query_with_rejecting_allocation_policy() -> None:
    # this test does not need the db or a query because the allocation policy
    # should reject the query before it gets to execution
    class RejectAllocationPolicy(AllocationPolicy):
        def _additional_config_definitions(self) -> list[AllocationPolicyConfig]:
            return []

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
        "snuba.web.db_query._get_allocation_policies",
        return_value=[
            RejectAllocationPolicy(StorageKey("doesntmatter"), ["a", "b", "c"], {})
        ],
    ):
        query_metadata_list: list[ClickhouseQueryMetadata] = []
        stats: dict[str, Any] = {}
        with pytest.raises(QueryException) as excinfo:
            db_query(
                clickhouse_query=mock.Mock(),
                query_settings=HTTPQuerySettings(),
                attribution_info=mock.Mock(),
                dataset_name="events",
                query_metadata_list=query_metadata_list,
                formatted_query=mock.Mock(),
                reader=mock.Mock(),
                timer=Timer("foo"),
                stats=stats,
                trace_id="trace_id",
                robust=False,
            )
        assert stats["quota_allowance"] == {
            "RejectAllocationPolicy": {
                "can_run": False,
                "explanation": {"reason": "policy rejects all queries"},
                "max_threads": 0,
            }
        }
        # extra data contains policy failure information
        assert (
            excinfo.value.extra["stats"]["quota_allowance"]["RejectAllocationPolicy"][
                "explanation"
            ]["reason"]
            == "policy rejects all queries"
        )
        cause = excinfo.value.__cause__
        assert isinstance(cause, AllocationPolicyViolations)
        assert "RejectAllocationPolicy" in cause.violations


@pytest.mark.clickhouse_db
@pytest.mark.redis_db
def test_allocation_policy_threads_applied_to_query() -> None:
    POLICY_THREADS = 4

    class ThreadLimitPolicy(AllocationPolicy):
        def _additional_config_definitions(self) -> list[AllocationPolicyConfig]:
            return []

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

    class ThreadLimitPolicyDuplicate(ThreadLimitPolicy):
        def _get_quota_allowance(
            self, tenant_ids: dict[str, str | int]
        ) -> QuotaAllowance:
            return QuotaAllowance(
                can_run=True,
                max_threads=POLICY_THREADS + 1,
                explanation={"reason": "Throttle everything!"},
            )

    # Should limit to minimal threads across policies
    query, storage, attribution_info = _build_test_query(
        "count(distinct(project_id))",
        [
            ThreadLimitPolicy(StorageKey("doesntmatter"), ["a", "b", "c"], {}),
            ThreadLimitPolicyDuplicate(StorageKey("doesntmatter"), ["a", "b", "c"], {}),
        ],
    )

    query_metadata_list: list[ClickhouseQueryMetadata] = []
    stats: dict[str, Any] = {}
    settings = HTTPQuerySettings()
    settings.set_resource_quota(ResourceQuota(max_threads=420))
    db_query(
        clickhouse_query=query,
        query_settings=settings,
        attribution_info=attribution_info,
        dataset_name="events",
        query_metadata_list=query_metadata_list,
        formatted_query=format_query(query),
        reader=storage.get_cluster().get_reader(),
        timer=Timer("foo"),
        stats=stats,
        trace_id="trace_id",
        robust=False,
    )
    assert settings.get_resource_quota().max_threads == POLICY_THREADS  # type: ignore
    assert stats["max_threads"] == POLICY_THREADS
    assert query_metadata_list[0].stats["max_threads"] == POLICY_THREADS


@pytest.mark.clickhouse_db
@pytest.mark.redis_db
def test_allocation_policy_updates_quota() -> None:
    MAX_QUERIES_TO_RUN = 2

    queries_run = 0

    class CountQueryPolicy(AllocationPolicy):
        def _additional_config_definitions(self) -> list[AllocationPolicyConfig]:
            return []

        def _get_quota_allowance(
            self, tenant_ids: dict[str, str | int]
        ) -> QuotaAllowance:
            can_run = True
            if queries_run + 1 > MAX_QUERIES_TO_RUN:
                can_run = False
            return QuotaAllowance(
                can_run=can_run,
                max_threads=0,
                explanation={"reason": f"can only run {queries_run} queries!"},
            )

        def _update_quota_balance(
            self,
            tenant_ids: dict[str, str | int],
            result_or_error: QueryResultOrError,
        ) -> None:
            nonlocal queries_run
            queries_run += 1

    queries_run_duplicate = 0

    class CountQueryPolicyDuplicate(AllocationPolicy):
        def _additional_config_definitions(self) -> list[AllocationPolicyConfig]:
            return []

        def _get_quota_allowance(
            self, tenant_ids: dict[str, str | int]
        ) -> QuotaAllowance:
            can_run = True
            if queries_run_duplicate + 1 > MAX_QUERIES_TO_RUN:
                can_run = False
            return QuotaAllowance(
                can_run=can_run,
                max_threads=0,
                explanation={
                    "reason": f"can only run {queries_run_duplicate} queries!"
                },
            )

        def _update_quota_balance(
            self,
            tenant_ids: dict[str, str | int],
            result_or_error: QueryResultOrError,
        ) -> None:
            nonlocal queries_run_duplicate
            queries_run_duplicate += 1

    # both policies should error
    query, storage, attribution_info = _build_test_query(
        "count(distinct(project_id))",
        [
            CountQueryPolicy(StorageKey("doesntmatter"), ["a", "b", "c"], {}),
            CountQueryPolicyDuplicate(StorageKey("doesntmatter"), ["a", "b", "c"], {}),
        ],
    )

    def _run_query() -> None:
        query_metadata_list: list[ClickhouseQueryMetadata] = []
        stats: dict[str, Any] = {}
        settings = HTTPQuerySettings()
        db_query(
            clickhouse_query=query,
            query_settings=settings,
            attribution_info=attribution_info,
            dataset_name="events",
            query_metadata_list=query_metadata_list,
            formatted_query=format_query(query),
            reader=storage.get_cluster().get_reader(),
            timer=Timer("foo"),
            stats=stats,
            trace_id="trace_id",
            robust=False,
        )

    for _ in range(MAX_QUERIES_TO_RUN):
        _run_query()
    with pytest.raises(QueryException) as e:
        _run_query()

    assert e.value.extra["stats"]["quota_allowance"] == {
        "CountQueryPolicy": {
            "can_run": False,
            "max_threads": 0,
            "explanation": {"reason": "can only run 2 queries!"},
        },
        "CountQueryPolicyDuplicate": {
            "can_run": False,
            "max_threads": 0,
            "explanation": {"reason": "can only run 2 queries!"},
        },
    }
    cause = e.value.__cause__
    assert isinstance(cause, AllocationPolicyViolations)
    assert "CountQueryPolicy" in cause.violations
    assert "CountQueryPolicyDuplicate" in cause.violations


@pytest.mark.redis_db
def test_clickhouse_settings_applied_to_query() -> None:
    query, storage, attribution_info = _build_test_query("count(distinct(project_id))")

    query_metadata_list: list[ClickhouseQueryMetadata] = []
    stats: dict[str, Any] = {}

    settings = HTTPQuerySettings()
    clickhouse_settings = {
        "max_rows_to_group_by": 1000000,
        "group_by_overflow_mode": "any",
    }
    settings.set_clickhouse_settings(clickhouse_settings)

    reader = mock.MagicMock()
    result = mock.MagicMock()
    reader.execute.return_value = result
    result.get.return_value.get.return_value = 0

    db_query(
        clickhouse_query=query,
        query_settings=settings,
        attribution_info=attribution_info,
        dataset_name="events",
        query_metadata_list=query_metadata_list,
        formatted_query=format_query(query),
        reader=reader,
        timer=Timer("foo"),
        stats=stats,
        trace_id="trace_id",
        robust=False,
    )

    clickhouse_settings_used = reader.execute.call_args.args[1]
    assert (
        "max_rows_to_group_by" in clickhouse_settings_used
        and clickhouse_settings_used["max_rows_to_group_by"] == 1000000
    )
    assert (
        "group_by_overflow_mode" in clickhouse_settings_used
        and clickhouse_settings_used["group_by_overflow_mode"] == "any"
    )
