from unittest.mock import MagicMock, patch

from flask import json

from snuba.admin.clickhouse.profile_events import (
    _profile_event_query_ids,
    gather_profile_events,
)
from snuba.clickhouse.native import ClickhouseProfile, ClickhouseResult


def test_profile_event_query_ids_from_summaries_and_root() -> None:
    trace_output = MagicMock()
    trace_output.summarized_trace_output.query_summaries = {
        "host1": MagicMock(query_id="query1"),
        "host2": MagicMock(query_id="query2"),
    }
    trace_output.query_id = "root-query-id"

    assert _profile_event_query_ids(trace_output) == [
        "query1",
        "query2",
        "root-query-id",
    ]


def test_profile_event_query_ids_root_only() -> None:
    trace_output = MagicMock()
    trace_output.summarized_trace_output.query_summaries = {}
    trace_output.query_id = "root-query-id"

    assert _profile_event_query_ids(trace_output) == ["root-query-id"]


def test_gather_profile_events() -> None:
    trace_output = MagicMock()
    trace_output.summarized_trace_output.query_summaries = {
        "host1": MagicMock(query_id="query1"),
    }
    trace_output.query_id = "query1"
    trace_output.profile_events_meta = []
    trace_output.profile_events_results = {}

    mock_connection = MagicMock()
    mock_connection.execute.return_value = ClickhouseResult(
        results=[("host1", {"SelectedRows": 1})],
        meta=[("host", "String"), ("ProfileEvents", "Map(String, UInt64)")],
        profile=ClickhouseProfile(bytes=0, progress_bytes=0, blocks=0, rows=1, elapsed=0.1),
    )

    with (
        patch(
            "snuba.admin.clickhouse.profile_events.get_ro_query_node_connection",
            return_value=mock_connection,
        ),
        patch(
            "snuba.admin.clickhouse.profile_events.system_log_source",
            return_value="system.query_log",
        ),
    ):
        gather_profile_events(trace_output, "test_storage")

    mock_connection.execute.assert_called_once()
    sql = mock_connection.execute.call_args.kwargs["query"]
    assert "ProfileEvents" in sql
    assert "query_id IN ('query1')" in sql
    assert trace_output.profile_events_meta == [
        [("host", "String"), ("ProfileEvents", "Map(String, UInt64)")]
    ]
    assert trace_output.profile_events_profile == {
        "bytes": 0,
        "progress_bytes": 0,
        "blocks": 0,
        "rows": 1,
        "elapsed": 0.1,
    }
    assert trace_output.profile_events_results["host1"] == {
        "column_names": ["host", "ProfileEvents"],
        "rows": [json.dumps({"SelectedRows": 1})],
    }


def test_gather_profile_events_retry_logic() -> None:
    trace_output = MagicMock()
    trace_output.summarized_trace_output.query_summaries = {
        "host1": MagicMock(query_id="query1"),
    }
    trace_output.query_id = "query1"
    trace_output.profile_events_meta = []
    trace_output.profile_events_results = {}

    empty_result = ClickhouseResult(results=[])
    success_result = ClickhouseResult(
        results=[("host1", {"SelectedRows": 1})],
        meta=[("host", "String"), ("ProfileEvents", "Map(String, UInt64)")],
        profile=ClickhouseProfile(bytes=0, progress_bytes=0, blocks=0, rows=1, elapsed=0.1),
    )

    mock_connection = MagicMock()
    mock_connection.execute.side_effect = [empty_result, empty_result, success_result]

    with (
        patch(
            "snuba.admin.clickhouse.profile_events.get_ro_query_node_connection",
            return_value=mock_connection,
        ),
        patch(
            "snuba.admin.clickhouse.profile_events.system_log_source",
            return_value="system.query_log",
        ),
        patch("time.sleep") as mock_sleep,
    ):
        gather_profile_events(trace_output, "test_storage")

    assert mock_connection.execute.call_count == 3
    assert mock_sleep.call_count == 2
    assert mock_sleep.call_args_list[0][0][0] == 2
    assert mock_sleep.call_args_list[1][0][0] == 4
