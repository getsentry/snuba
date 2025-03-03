from __future__ import annotations

from typing import Any, Mapping, MutableMapping, Optional
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
    MAX_THRESHOLD,
    NO_SUGGESTION,
    NO_UNITS,
    AllocationPolicy,
    AllocationPolicyConfig,
    AllocationPolicyViolations,
    PassthroughPolicy,
    QueryResultOrError,
    QuotaAllowance,
)
from snuba.query.data_source.simple import Table
from snuba.query.parser.expressions import parse_clickhouse_function
from snuba.query.query_settings import HTTPQuerySettings
from snuba.querylog.query_metadata import ClickhouseQueryMetadata
from snuba.state.quota import ResourceQuota
from snuba.utils.metrics.backends.testing import get_recorded_metric_calls
from snuba.utils.metrics.timer import Timer
from snuba.web import QueryException
from snuba.web.db_query import (
    _apply_allocation_policies_quota,
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
        False,
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
        False,
        None,
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
        False,
        None,
        id="override for same query prefix",
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
        None,
        True,
        None,
        id="no override when no async settings",
    ),
    pytest.param(
        {
            "query_settings/max_threads": 10,
            "query_settings/merge_tree_max_rows_to_use_cache": 50000,
            "some-query-prefix/query_settings/merge_tree_max_rows_to_use_cache": 100000,
            "async_query_settings/max_threads": 20,
        },
        {
            "max_threads": 20,
            "merge_tree_max_rows_to_use_cache": 50000,
        },
        "other-query-prefix",
        True,
        None,
        id="override for async settings with no prefix override",
    ),
    pytest.param(
        {
            "query_settings/max_threads": 10,
            "query_settings/merge_tree_max_rows_to_use_cache": 50000,
            "some-query-prefix/query_settings/max_threads": 5,
            "some-query-prefix/query_settings/merge_tree_max_rows_to_use_cache": 100000,
            "async_query_settings/max_threads": 20,
        },
        {
            "max_threads": 5,
            "merge_tree_max_rows_to_use_cache": 100000,
        },
        "some-query-prefix",
        True,
        None,
        id="no override for async settings with prefix override",
    ),
    pytest.param(
        {
            "query_settings/max_threads": 10,
            "some-query-prefix/query_settings/merge_tree_max_rows_to_use_cache": 50000,
            "referrer/some-referrer/query_settings/max_read_replicas": 4,
        },
        {
            "max_threads": 10,
            "merge_tree_max_rows_to_use_cache": 50000,
            "max_read_replicas": 4,
        },
        "some-query-prefix",
        True,
        "some-referrer",
        id="referrer override does its job",
    ),
    pytest.param(
        {
            "query_settings/max_threads": 10,
            "query_settings/merge_tree_max_rows_to_use_cache": 50000,
            "some-query-prefix/query_settings/max_threads": 5,
            "some-query-prefix/query_settings/merge_tree_max_rows_to_use_cache": 100000,
            "async_query_settings/max_threads": 20,
            "referrer/some-referrer/query_settings/max_threads": 30,
        },
        {
            "max_threads": 30,
            "merge_tree_max_rows_to_use_cache": 100000,
        },
        "some-query-prefix",
        True,
        "some-referrer",
        id="referrer override takes precedence over all other settings",
    ),
    pytest.param(
        {
            "query_settings/max_threads": 10,
            "query_settings/merge_tree_max_rows_to_use_cache": 50000,
            "some-query-prefix/query_settings/max_threads": 5,
            "some-query-prefix/query_settings/merge_tree_max_rows_to_use_cache": 100000,
            "async_query_settings/max_threads": 20,
            "referrer/some-referrer/query_settings/max_threads": 30,
        },
        {
            "max_threads": 5,
            "merge_tree_max_rows_to_use_cache": 100000,
        },
        "some-query-prefix",
        True,
        "some-other-referrer",
        id="referrer override does not apply to other referrers",
    ),
]


@pytest.mark.parametrize(
    "query_config,expected,query_prefix,async_override,referrer", test_data
)
@pytest.mark.redis_db
def test_query_settings_from_config(
    query_config: Mapping[str, Any],
    expected: MutableMapping[str, Any],
    query_prefix: Optional[str],
    async_override: bool,
    referrer: str,
) -> None:
    for k, v in query_config.items():
        state.set_config(k, v)
    assert (
        _get_query_settings_from_config(query_prefix, async_override, referrer=referrer)
        == expected
    )


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
                storage_key=storage.get_storage_key(),
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
def test_db_record_bytes_scanned() -> None:
    dataset_name = "events"
    storage_key = StorageKey("errors_ro")
    query, storage, attribution_info = _build_test_query(
        "count(distinct(project_id))",
        allocation_policies=[PassthroughPolicy(storage_key, [], {})],
    )

    query_metadata_list: list[ClickhouseQueryMetadata] = []
    stats: dict[str, Any] = {}

    db_query(
        clickhouse_query=query,
        query_settings=HTTPQuerySettings(),
        attribution_info=attribution_info,
        dataset_name=dataset_name,
        query_metadata_list=query_metadata_list,
        formatted_query=format_query(query),
        reader=storage.get_cluster().get_reader(),
        timer=Timer("foo"),
        stats=stats,
        trace_id="trace_id",
        robust=False,
    )

    metrics = get_recorded_metric_calls("increment", "allocation_policy.bytes_scanned")
    assert metrics
    assert len(metrics) == 1
    assert metrics[0].tags == {
        "referrer": attribution_info.referrer,
        "dataset": dataset_name,
        "storage_key": storage_key.value,
    }


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
        "summary": {
            "threads_used": 10,
            "is_successful": True,
            "is_rejected": False,
            "is_throttled": False,
            "rejection_storage_key": None,
            "throttle_storage_key": None,
            "rejected_by": {},
            "throttled_by": {},
        },
        "details": {
            "ReferrerGuardRailPolicy": {
                "can_run": True,
                "max_threads": 10,
                "max_bytes_to_read": 0,
                "explanation": {
                    "reason": "within limit",
                    "policy": "referrer_guard_rail_policy",
                    "referrer": "something",
                    "storage_key": "StorageKey.ERRORS_RO",
                },
                "is_throttled": False,
                "throttle_threshold": 66,
                "rejection_threshold": 100,
                "quota_used": 1,
                "quota_unit": "concurrent_queries",
                "suggestion": NO_SUGGESTION,
            },
            "ConcurrentRateLimitAllocationPolicy": {
                "can_run": True,
                "max_threads": 10,
                "max_bytes_to_read": 0,
                "explanation": {
                    "reason": "within limit",
                    "overrides": {},
                    "storage_key": "StorageKey.ERRORS_RO",
                },
                "is_throttled": False,
                "throttle_threshold": 22,
                "rejection_threshold": 22,
                "quota_used": 1,
                "quota_unit": "concurrent_queries",
                "suggestion": NO_SUGGESTION,
            },
            "BytesScannedRejectingPolicy": {
                "can_run": True,
                "max_threads": 10,
                "max_bytes_to_read": 0,
                "explanation": {
                    "reason": "within_limit",
                    "storage_key": "StorageKey.ERRORS_RO",
                },
                "is_throttled": False,
                "throttle_threshold": 1706666666666,
                "rejection_threshold": 2560000000000,
                "quota_used": 1560000000000,
                "quota_unit": "bytes",
                "suggestion": "no_suggestion",
            },
            "CrossOrgQueryAllocationPolicy": {
                "can_run": True,
                "max_bytes_to_read": 0,
                "max_threads": 10,
                "explanation": {
                    "reason": "pass_through",
                    "storage_key": "StorageKey.ERRORS_RO",
                },
                "is_throttled": False,
                "throttle_threshold": MAX_THRESHOLD,
                "rejection_threshold": MAX_THRESHOLD,
                "quota_used": 0,
                "quota_unit": NO_UNITS,
                "suggestion": NO_SUGGESTION,
            },
        },
    }

    assert len(query_metadata_list) == 1
    assert result.extra["stats"] == stats
    assert result.extra["sql"] is not None
    assert set(result.result["profile"].keys()) == {  # type: ignore
        "elapsed",
        "bytes",
        "progress_bytes",
        "blocks",
        "rows",
    }


@pytest.mark.clickhouse_db
@pytest.mark.redis_db
def test_bypass_cache_referrer() -> None:
    query, storage, _ = _build_test_query("count(distinct(project_id))")

    query_metadata_list: list[ClickhouseQueryMetadata] = []
    stats: dict[str, Any] = {"clickhouse_table": "errors_local"}

    state.set_config("enable_bypass_cache_referrers", 1)

    attribution_info = AttributionInfo(
        app_id=AppID(key="key"),
        tenant_ids={
            "referrer": "some_bypass_cache_referrer",
            "organization_id": 1234,
        },
        referrer="some_bypass_cache_referrer",
        team=None,
        feature=None,
        parent_api=None,
    )

    # cache should not be used for "some_bypass_cache_referrer" so if the
    # bypass does not work, the test will try to use a bad cache
    with mock.patch(
        "snuba.settings.BYPASS_CACHE_REFERRERS", ["some_bypass_cache_referrer"]
    ):
        with mock.patch("snuba.web.db_query._get_cache_partition"):
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
            assert len(query_metadata_list) == 1
            assert result.extra["stats"] == stats
            assert result.extra["sql"] is not None
            assert set(result.result["profile"].keys()) == {  # type: ignore
                "elapsed",
                "bytes",
                "progress_bytes",
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
    assert query_metadata_list[0].status.value == "error"
    assert excinfo.value.extra["stats"] == stats
    assert excinfo.value.extra["sql"] is not None


class MockThrottleAllocationPolicy(AllocationPolicy):
    def __init__(
        self,
        max_threads: int,
        policy_name: str,
        storage_key: StorageKey = StorageKey("doesntmatter"),
        required_tenant_types: list[str] = ["a", "b", "c"],
        default_config_overrides: dict[str, Any] = {},
    ) -> None:
        super().__init__(
            storage_key=storage_key,
            required_tenant_types=required_tenant_types,
            default_config_overrides=default_config_overrides,
        )
        self._max_threads = max_threads
        self.policy_name = policy_name

    def _get_quota_allowance(
        self, tenant_ids: dict[str, str | int], query_id: str
    ) -> QuotaAllowance:
        return QuotaAllowance(
            can_run=True,
            max_threads=self._max_threads,
            explanation={"reason": self.policy_name + " throttles all queries"},
            is_throttled=True,
            throttle_threshold=MAX_THRESHOLD,
            rejection_threshold=MAX_THRESHOLD + 1,
            quota_used=MAX_THRESHOLD,
            quota_unit=NO_UNITS,
            suggestion=NO_SUGGESTION,
        )

    def _update_quota_balance(
        self,
        tenant_ids: dict[str, str | int],
        query_id: str,
        result_or_error: QueryResultOrError,
    ) -> None:
        return

    def _additional_config_definitions(self) -> list[AllocationPolicyConfig]:
        return []


def test_apply_allocation_policies_quota_sets_throttle_policy() -> None:
    query, _, _ = _build_test_query("count(distinct(project_id))")

    class ThrottleAllocationPolicy1(MockThrottleAllocationPolicy):
        def __init__(self, max_threads: int, policy_name: str) -> None:
            super().__init__(max_threads=max_threads, policy_name=policy_name)

    class ThrottleAllocationPolicy2(MockThrottleAllocationPolicy):
        def __init__(self, max_threads: int, policy_name: str) -> None:
            super().__init__(max_threads=max_threads, policy_name=policy_name)

    stats: MutableMapping[str, Any] = {}
    _apply_allocation_policies_quota(
        query_settings=HTTPQuerySettings(),
        attribution_info=mock.Mock(),
        formatted_query=format_query(query),
        stats=stats,
        allocation_policies=[
            ThrottleAllocationPolicy1(1, "ThrottleAllocationPolicy1"),
            ThrottleAllocationPolicy2(2, "ThrottleAllocationPolicy2"),
        ],
        query_id="throttle_query",
    )

    assert stats == {
        "quota_allowance": {
            "details": {
                "ThrottleAllocationPolicy1": {
                    "can_run": True,
                    "max_threads": 1,
                    "max_bytes_to_read": 0,
                    "explanation": {
                        "reason": "ThrottleAllocationPolicy1 throttles all queries",
                        "storage_key": "StorageKey.DOESNTMATTER",
                    },
                    "is_throttled": True,
                    "throttle_threshold": 1000000000000,
                    "rejection_threshold": 1000000000001,
                    "quota_used": 1000000000000,
                    "quota_unit": NO_UNITS,
                    "suggestion": NO_SUGGESTION,
                },
                "ThrottleAllocationPolicy2": {
                    "can_run": True,
                    "max_threads": 2,
                    "max_bytes_to_read": 0,
                    "explanation": {
                        "reason": "ThrottleAllocationPolicy2 throttles all queries",
                        "storage_key": "StorageKey.DOESNTMATTER",
                    },
                    "is_throttled": True,
                    "throttle_threshold": 1000000000000,
                    "rejection_threshold": 1000000000001,
                    "quota_used": 1000000000000,
                    "quota_unit": NO_UNITS,
                    "suggestion": NO_SUGGESTION,
                },
            },
            "summary": {
                "threads_used": 1,
                "is_successful": False,
                "is_rejected": False,
                "is_throttled": True,
                "rejection_storage_key": None,
                "throttle_storage_key": "StorageKey.DOESNTMATTER",
                "rejected_by": {},
                "throttled_by": {
                    "policy": "ThrottleAllocationPolicy1",
                    "quota_used": 1000000000000,
                    "quota_unit": NO_UNITS,
                    "suggestion": NO_SUGGESTION,
                    "throttle_threshold": 1000000000000,
                    "storage_key": "StorageKey.DOESNTMATTER",
                },
            },
        }
    }


def test_db_query_with_rejecting_allocation_policy() -> None:
    # this test does not need the db or a query because the allocation policy
    # should reject the query before it gets to execution
    query, storage, _ = _build_test_query("count(distinct(project_id))")
    update_called = False

    class RejectAllocationPolicy(AllocationPolicy):
        def _additional_config_definitions(self) -> list[AllocationPolicyConfig]:
            return []

        def _get_quota_allowance(
            self, tenant_ids: dict[str, str | int], query_id: str
        ) -> QuotaAllowance:
            return QuotaAllowance(
                can_run=False,
                max_threads=0,
                explanation={"reason": "policy rejects all queries"},
                is_throttled=True,
                throttle_threshold=MAX_THRESHOLD,
                rejection_threshold=MAX_THRESHOLD,
                quota_used=0,
                quota_unit=NO_UNITS,
                suggestion=NO_SUGGESTION,
            )

        def _update_quota_balance(
            self,
            tenant_ids: dict[str, str | int],
            query_id: str,
            result_or_error: QueryResultOrError,
        ) -> None:
            nonlocal update_called
            update_called = True
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
                clickhouse_query=query,
                query_settings=HTTPQuerySettings(),
                attribution_info=mock.Mock(),
                dataset_name="events",
                query_metadata_list=query_metadata_list,
                formatted_query=format_query(query),
                reader=mock.Mock(),
                timer=Timer("foo"),
                stats=stats,
                trace_id="trace_id",
                robust=False,
            )
        assert stats["quota_allowance"] == {
            "summary": {
                "threads_used": 0,
                "is_successful": False,
                "is_rejected": True,
                "is_throttled": True,
                "rejection_storage_key": "StorageKey.DOESNTMATTER",
                "throttle_storage_key": "StorageKey.DOESNTMATTER",
                "rejected_by": {
                    "policy": "RejectAllocationPolicy",
                    "rejection_threshold": MAX_THRESHOLD,
                    "quota_used": 0,
                    "quota_unit": NO_UNITS,
                    "suggestion": NO_SUGGESTION,
                    "storage_key": "StorageKey.DOESNTMATTER",
                },
                "throttled_by": {
                    "policy": "RejectAllocationPolicy",
                    "throttle_threshold": MAX_THRESHOLD,
                    "quota_used": 0,
                    "quota_unit": NO_UNITS,
                    "suggestion": NO_SUGGESTION,
                    "storage_key": "StorageKey.DOESNTMATTER",
                },
            },
            "details": {
                "RejectAllocationPolicy": {
                    "can_run": False,
                    "max_bytes_to_read": 0,
                    "explanation": {
                        "reason": "policy rejects all queries",
                        "storage_key": "StorageKey.DOESNTMATTER",
                    },
                    "max_threads": 0,
                    "is_throttled": True,
                    "quota_unit": NO_UNITS,
                    "quota_used": 0,
                    "rejection_threshold": MAX_THRESHOLD,
                    "suggestion": NO_SUGGESTION,
                    "throttle_threshold": MAX_THRESHOLD,
                },
            },
        }
        # extra data contains policy failure information
        assert (
            excinfo.value.extra["stats"]["quota_allowance"]["details"][
                "RejectAllocationPolicy"
            ]["explanation"]["reason"]
            == "policy rejects all queries"
        )
        assert query_metadata_list[0].request_status.status.value == "rate-limited"
        cause = excinfo.value.__cause__
        assert isinstance(cause, AllocationPolicyViolations)
        assert "RejectAllocationPolicy" in cause.violations
        assert (
            update_called
        ), "update_quota_balance should have been called even though the query was rejected but was not"


@pytest.mark.clickhouse_db
@pytest.mark.redis_db
def test_allocation_policy_threads_applied_to_query() -> None:
    POLICY_THREADS = 4

    class ThreadLimitPolicy(AllocationPolicy):
        def _additional_config_definitions(self) -> list[AllocationPolicyConfig]:
            return []

        def _get_quota_allowance(
            self, tenant_ids: dict[str, str | int], query_id: str
        ) -> QuotaAllowance:
            return QuotaAllowance(
                can_run=True,
                max_threads=POLICY_THREADS,
                explanation={"reason": "Throttle everything!"},
                is_throttled=True,
                throttle_threshold=MAX_THRESHOLD,
                rejection_threshold=MAX_THRESHOLD,
                quota_used=0,
                quota_unit=NO_UNITS,
                suggestion=NO_SUGGESTION,
            )

        def _update_quota_balance(
            self,
            tenant_ids: dict[str, str | int],
            query_id: str,
            result_or_error: QueryResultOrError,
        ) -> None:
            return

    class ThreadLimitPolicyDuplicate(ThreadLimitPolicy):
        def _get_quota_allowance(
            self, tenant_ids: dict[str, str | int], query_id: str
        ) -> QuotaAllowance:
            return QuotaAllowance(
                can_run=True,
                max_threads=POLICY_THREADS + 1,
                explanation={"reason": "Throttle everything!"},
                is_throttled=True,
                throttle_threshold=MAX_THRESHOLD,
                rejection_threshold=MAX_THRESHOLD,
                quota_used=0,
                quota_unit=NO_UNITS,
                suggestion=NO_SUGGESTION,
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
            self, tenant_ids: dict[str, str | int], query_id: str
        ) -> QuotaAllowance:
            can_run = True
            suggestion = NO_SUGGESTION
            if queries_run + 1 > MAX_QUERIES_TO_RUN:
                can_run = False
                suggestion = "scan less concurrent queries"
            return QuotaAllowance(
                can_run=can_run,
                max_threads=0,
                explanation={"reason": f"can only run {queries_run} queries!"},
                is_throttled=False,
                throttle_threshold=MAX_QUERIES_TO_RUN,
                rejection_threshold=MAX_QUERIES_TO_RUN,
                quota_used=queries_run + 1,
                quota_unit="queries",
                suggestion=suggestion,
            )

        def _update_quota_balance(
            self,
            tenant_ids: dict[str, str | int],
            query_id: str,
            result_or_error: QueryResultOrError,
        ) -> None:
            nonlocal queries_run
            queries_run += 1

    queries_run_duplicate = 0

    class CountQueryPolicyDuplicate(AllocationPolicy):
        def _additional_config_definitions(self) -> list[AllocationPolicyConfig]:
            return []

        def _get_quota_allowance(
            self, tenant_ids: dict[str, str | int], query_id: str
        ) -> QuotaAllowance:
            can_run = True
            suggestion = NO_SUGGESTION
            if queries_run_duplicate + 1 > MAX_QUERIES_TO_RUN:
                can_run = False
                suggestion = "scan less concurrent queries"

            return QuotaAllowance(
                can_run=can_run,
                max_threads=0,
                explanation={
                    "reason": f"can only run {queries_run_duplicate} queries!"
                },
                is_throttled=False,
                throttle_threshold=MAX_QUERIES_TO_RUN,
                rejection_threshold=MAX_QUERIES_TO_RUN,
                quota_used=queries_run + 1,
                quota_unit="queries",
                suggestion=suggestion,
            )

        def _update_quota_balance(
            self,
            tenant_ids: dict[str, str | int],
            query_id: str,
            result_or_error: QueryResultOrError,
        ) -> None:
            nonlocal queries_run_duplicate
            queries_run_duplicate += 1

    # the first policy will error and short circuit the rest
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
        "summary": {
            "threads_used": 0,
            "is_successful": False,
            "is_rejected": True,
            "is_throttled": False,
            "rejection_storage_key": "StorageKey.DOESNTMATTER",
            "throttle_storage_key": None,
            "rejected_by": {
                "policy": "CountQueryPolicy",
                "rejection_threshold": MAX_QUERIES_TO_RUN,
                "quota_used": queries_run,
                "quota_unit": "queries",
                "suggestion": "scan less concurrent queries",
                "storage_key": "StorageKey.DOESNTMATTER",
            },
            "throttled_by": {},
        },
        "details": {
            "CountQueryPolicy": {
                "can_run": False,
                "max_threads": 0,
                "explanation": {
                    "reason": "can only run 2 queries!",
                    "storage_key": "StorageKey.DOESNTMATTER",
                },
                "is_throttled": False,
                "max_bytes_to_read": 0,
                "throttle_threshold": MAX_QUERIES_TO_RUN,
                "rejection_threshold": MAX_QUERIES_TO_RUN,
                "quota_used": queries_run,
                "quota_unit": "queries",
                "suggestion": "scan less concurrent queries",
            },
        },
    }
    cause = e.value.__cause__
    assert isinstance(cause, AllocationPolicyViolations)
    assert "CountQueryPolicy" in cause.violations
    assert "CountQueryPolicyDuplicate" not in cause.violations


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


@pytest.mark.clickhouse_db
@pytest.mark.redis_db
def test_db_query_ignore_consistent() -> None:
    query, storage, attribution_info = _build_test_query("count(distinct(project_id))")
    state.set_config("events_ignore_consistent_queries_sample_rate", 1)

    query_metadata_list: list[ClickhouseQueryMetadata] = []
    stats: dict[str, Any] = {}

    result = db_query(
        clickhouse_query=query,
        query_settings=HTTPQuerySettings(consistent=True),
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
    assert result.extra["stats"]["consistent"] is False
    assert result.extra["stats"]["max_threads"] == 10


@pytest.mark.clickhouse_db
@pytest.mark.redis_db
def test_cache_metrics_with_simple_readthrough() -> None:
    query, storage, attribution_info = _build_test_query("count(distinct(project_id))")
    state.set_config("disable_lua_randomize_query_id", 1)
    state.set_config("read_through_cache.disable_lua_scripts_sample_rate", 1)

    formatted_query = format_query(query)
    reader = storage.get_cluster().get_reader()

    with mock.patch("snuba.web.db_query.metrics", new=mock.Mock()) as metrics_mock:
        result = db_query(
            clickhouse_query=query,
            query_settings=HTTPQuerySettings(),
            attribution_info=attribution_info,
            dataset_name="events",
            query_metadata_list=[],
            formatted_query=formatted_query,
            reader=reader,
            timer=Timer("foo"),
            stats={},
            trace_id="trace_id",
            robust=False,
        )
        assert "cache_hit_simple" in result.extra["stats"]
        # Assert on first call cache_miss is incremented
        metrics_mock.assert_has_calls(
            [
                mock.call.increment("cache_miss", tags={"dataset": "events"}),
                mock.call.increment("cache_hit_simple", tags={"dataset": "events"}),
            ]
        )

        metrics_mock.reset_mock()
        result = db_query(
            clickhouse_query=query,
            query_settings=HTTPQuerySettings(),
            attribution_info=attribution_info,
            dataset_name="events",
            query_metadata_list=[],
            formatted_query=formatted_query,
            reader=reader,
            timer=Timer("foo"),
            stats={},
            trace_id="trace_id",
            robust=False,
        )
        assert "cache_hit_simple" in result.extra["stats"]
        # Assert on second call cache_hit is incremented
        metrics_mock.assert_has_calls(
            [
                mock.call.increment("cache_hit", tags={"dataset": "events"}),
                mock.call.increment("cache_hit_simple", tags={"dataset": "events"}),
            ]
        )
