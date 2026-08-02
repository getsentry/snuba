from __future__ import annotations

import json
from typing import Any
from unittest.mock import patch

import pytest
from google.protobuf.json_format import MessageToDict
from sentry_protos.snuba.v1.endpoint_time_series_pb2 import (
    Expression as TimeSeriesExpression,
)
from sentry_protos.snuba.v1.endpoint_time_series_pb2 import TimeSeriesRequest
from sentry_protos.snuba.v1.endpoint_trace_item_table_pb2 import (
    Column,
    TraceItemTableRequest,
)
from sentry_protos.snuba.v1.request_common_pb2 import RequestMeta, TraceItemType
from sentry_protos.snuba.v1.trace_item_attribute_pb2 import (
    AttributeAggregation,
    AttributeKey,
    Function,
)

from snuba.admin.eap_query_analysis.analysis import (
    EapQueryAnalysisRequest,
    ResourceTotals,
    _build_fetch_sql,
    _escape_like_literal,
    _escape_literal,
    _infer_request_class,
    _parse_request_body,
    analyze_eap_queries,
    result_to_dict,
)
from snuba.clickhouse.native import ClickhouseResult


def _meta() -> RequestMeta:
    return RequestMeta(
        project_ids=[1],
        organization_id=1,
        referrer="test.referrer",
        trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
    )


def _table_body() -> dict[str, Any]:
    req = TraceItemTableRequest(
        meta=_meta(),
        columns=[
            Column(
                aggregation=AttributeAggregation(
                    aggregate=Function.FUNCTION_COUNT,
                    key=AttributeKey(type=AttributeKey.TYPE_DOUBLE, name="d"),
                    label="c",
                ),
                label="c",
            )
        ],
        group_by=[AttributeKey(type=AttributeKey.TYPE_STRING, name="op")],
    )
    return MessageToDict(req)


def _timeseries_body() -> dict[str, Any]:
    req = TimeSeriesRequest(
        meta=_meta(),
        granularity_secs=60,
        expressions=[
            TimeSeriesExpression(
                aggregation=AttributeAggregation(
                    aggregate=Function.FUNCTION_AVG,
                    key=AttributeKey(type=AttributeKey.TYPE_DOUBLE, name="d"),
                    label="avg",
                ),
                label="avg",
            )
        ],
    )
    return MessageToDict(req)


def test_infer_request_class() -> None:
    assert _infer_request_class(_table_body()) is TraceItemTableRequest
    assert _infer_request_class(_timeseries_body()) is TimeSeriesRequest
    assert _infer_request_class({"foo": "bar"}) is None

    # TimeSeriesRequest can also carry group_by; timeseries markers must win.
    timeseries_groupby = _timeseries_body()
    timeseries_groupby["groupBy"] = [{"name": "op", "type": "TYPE_STRING"}]
    assert _infer_request_class(timeseries_groupby) is TimeSeriesRequest


def test_parse_request_body() -> None:
    msg = _parse_request_body(json.dumps(_table_body()))
    assert msg is not None
    assert isinstance(msg, TraceItemTableRequest)
    assert len(msg.group_by) == 1


def test_resource_totals_add_profile_events() -> None:
    r = ResourceTotals()
    r.add_profile_events(
        {
            "UserTimeMicroseconds": 1000,
            "MemoryTrackerPeakUsage": 4096,
            "SelectedBytes": 100,
            "NetworkReceiveBytes": 50,
            "IgnoredEvent": 999,
        }
    )
    assert r.cpu_user_us == 1000
    assert r.memory_peak_bytes == 4096
    assert r.io_selected_bytes == 100
    assert r.network_receive_bytes == 50
    assert r.profile_events_matched == 1
    assert r.cpu_total_us == 1000

    # Virtual CPU time already covers threaded user/system work; prefer it.
    r2 = ResourceTotals()
    r2.add_profile_events(
        {
            "UserTimeMicroseconds": 1000,
            "SystemTimeMicroseconds": 500,
            "OSCPUVirtualTimeMicroseconds": 2000,
        }
    )
    assert r2.cpu_total_us == 2000


def test_analyze_eap_queries_categorizes_and_aggregates() -> None:
    table_body = json.dumps(_table_body())
    ts_body = json.dumps(_timeseries_body())

    # Columns match the SELECT in _build_fetch_sql.
    rows = [
        (
            "req-1",
            "2024-01-01 00:00:00",
            "api.table",
            "eap",
            1,
            120,
            "success",
            table_body,
            ["qid-aaaa"],
            [1_000_000],
            [100],
            None,
        ),
        (
            "req-2",
            "2024-01-01 00:01:00",
            "api.timeseries",
            "eap",
            1,
            250,
            "success",
            ts_body,
            ["qid-bbbb"],
            [5_000_000],
            [200],
            None,
        ),
        (
            "req-3",
            "2024-01-01 00:02:00",
            "api.unknown",
            "eap",
            1,
            10,
            "success",
            json.dumps({"not": "a request"}),
            [],
            [10],
            [5],
            None,
        ),
    ]

    mock_result = ClickhouseResult(results=rows, meta=[("x", "String")] * 12)

    profile_events = {
        "qid-aaaa": {
            "UserTimeMicroseconds": 2_000,
            "MemoryTrackerPeakUsage": 8_000,
            "SelectedBytes": 1_000_000,
            "NetworkReceiveBytes": 100,
        },
        "qidbbbb": {},  # unused
        "qid-bbbb": {
            "UserTimeMicroseconds": 8_000,
            "MemoryTrackerPeakUsage": 32_000,
            "SelectedBytes": 5_000_000,
            "NetworkReceiveBytes": 500,
        },
    }
    # keys are normalized without dashes in implementation
    profile_events_norm = {
        "qidaaaa": profile_events["qid-aaaa"],
        "qidbbbb": profile_events["qid-bbbb"],
    }

    with (
        patch(
            "snuba.admin.eap_query_analysis.analysis.run_querylog_query",
            return_value=mock_result,
        ) as mock_ql,
        patch(
            "snuba.admin.eap_query_analysis.analysis._schema_table_name",
            return_value="querylog_local",
        ),
        patch(
            "snuba.admin.eap_query_analysis.analysis._fetch_profile_events",
            return_value=profile_events_norm,
        ) as mock_pe,
    ):
        result = analyze_eap_queries(
            EapQueryAnalysisRequest(hours=1, max_rows=100, include_profile_events=True),
            user="tester@sentry.io",
        )

    mock_ql.assert_called_once()
    assert mock_ql.call_args.kwargs.get("max_threads") == 0
    mock_pe.assert_called_once()

    assert result.rows_scanned == 3
    assert result.rows_categorized == 2
    assert result.rows_failed == 1
    assert result.total_resources.bytes_scanned == 1_000_000 + 5_000_000 + 10
    assert result.total_resources.cpu_user_us == 10_000
    assert result.total_resources.memory_peak_bytes == 40_000
    assert result.profile_events_matched == 2

    coverage = result.profile_coverage
    assert coverage.enabled is True
    assert coverage.queries_total == 3
    assert coverage.queries_profiled == 2  # uncategorized row has no query ids matched
    assert coverage.queries_with_query_id == 2
    assert coverage.pct_queries_profiled == pytest.approx(200.0 / 3.0)
    assert coverage.bytes_profiled == 1_000_000 + 5_000_000
    assert coverage.pct_bytes_profiled == pytest.approx(
        100.0 * (1_000_000 + 5_000_000) / (1_000_000 + 5_000_000 + 10)
    )
    assert coverage.query_ids_matched == 2

    by_type = {b.query_type: b for b in result.by_query_type}
    assert "table_groupby" in by_type
    assert "timeseries" in by_type
    assert "uncategorized" in by_type

    # Timeseries scanned more bytes, so it should rank first.
    assert result.by_query_type[0].query_type == "timeseries"
    assert result.by_query_type[0].resources.bytes_scanned == 5_000_000
    assert result.by_query_type[0].pct_of_cpu > 0

    payload = result_to_dict(result)
    assert "by_query_type" in payload
    assert payload["total_resources"]["bytes_scanned"] == result.total_resources.bytes_scanned


def test_analyze_without_profile_events() -> None:
    rows = [
        (
            "req-1",
            "2024-01-01 00:00:00",
            "api.table",
            "eap",
            1,
            120,
            "success",
            json.dumps(_table_body()),
            ["qid-1"],
            [1000],
            [50],
            None,
        ),
    ]
    mock_result = ClickhouseResult(results=rows, meta=[])
    with (
        patch(
            "snuba.admin.eap_query_analysis.analysis.run_querylog_query",
            return_value=mock_result,
        ),
        patch(
            "snuba.admin.eap_query_analysis.analysis._schema_table_name",
            return_value="querylog_local",
        ),
        patch(
            "snuba.admin.eap_query_analysis.analysis._fetch_profile_events",
        ) as mock_pe,
    ):
        result = analyze_eap_queries(
            EapQueryAnalysisRequest(include_profile_events=False),
            user="tester",
        )
    mock_pe.assert_not_called()
    assert result.profile_events_enabled is False
    assert result.total_resources.cpu_user_us == 0
    assert result.total_resources.bytes_scanned == 1000


def test_request_from_dict_clamps() -> None:
    req = EapQueryAnalysisRequest.from_dict(
        {"hours": 9999, "max_rows": 9999999, "include_profile_events": False}
    )
    assert req.hours == 24 * 7
    assert req.max_rows == 500_000
    assert req.include_profile_events is False


def test_prefers_embedded_query_info_from_stats() -> None:
    stats = [
        json.dumps(
            {
                "query_info": {
                    "query_type": "table_cross_item",
                    "trace_item_type": "span",
                    "filter_profile": "like",
                    "has_groupby": "true",
                    "groupby_count": "1",
                    "has_formula": "false",
                    "has_cross_item": "true",
                }
            }
        )
    ]
    rows: list[tuple[Any, ...]] = [
        (
            "req-1",
            "2024-01-01 00:00:00",
            "api.x",
            "eap",
            1,
            10,
            "success",
            "{}",  # body intentionally unparseable / empty
            [],
            [42],
            [1],
            stats,
        )
    ]
    mock_result = ClickhouseResult(results=rows, meta=[])
    with (
        patch(
            "snuba.admin.eap_query_analysis.analysis.run_querylog_query",
            return_value=mock_result,
        ),
        patch(
            "snuba.admin.eap_query_analysis.analysis._schema_table_name",
            return_value="querylog_local",
        ),
        patch(
            "snuba.admin.eap_query_analysis.analysis._fetch_profile_events",
            return_value={},
        ),
    ):
        result = analyze_eap_queries(EapQueryAnalysisRequest(), user="t")

    assert result.rows_categorized == 1
    assert result.by_query_type[0].query_type == "table_cross_item"


def test_escape_like_literal_escapes_wildcards() -> None:
    assert _escape_literal("eap_items") == "eap_items"
    assert _escape_like_literal("eap_items") == "eap\\_items"
    assert _escape_like_literal("a%b_c'\\") == "a\\%b\\_c\\'\\\\"


def test_build_fetch_sql_filters_estimation_and_duplicates() -> None:
    with patch(
        "snuba.admin.eap_query_analysis.analysis._schema_table_name",
        return_value="querylog_local",
    ):
        sql = _build_fetch_sql(EapQueryAnalysisRequest(hours=1, referrer_contains="eap_items"))
    assert "dataset IN ('eap')" in sql
    assert "storage_routing" not in sql
    assert "positionCaseInsensitive(t, 'outcomes')" in sql
    assert "LIKE '%eap\\_items%'" in sql
    assert "ESCAPE '\\'" in sql
