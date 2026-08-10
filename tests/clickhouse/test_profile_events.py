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


def test_gather_profile_events_matches_legacy_payload_shape() -> None:
    """
    Frontend expects:
      profile_events_results[node_name] = {
        "column_names": ["ProfileEvents"],
        "rows": [json.dumps(profile_events_map), ...],
      }
    """
    trace_output = MagicMock()
    trace_output.summarized_trace_output.query_summaries = {
        "host1": MagicMock(query_id="query1"),
    }
    trace_output.query_id = "query1"
    trace_output.profile_events_meta = []
    trace_output.profile_events_results = {}

    profile_map = {"SelectedRows": 1, "Query": 1}
    mock_connection = MagicMock()
    mock_connection.execute.return_value = ClickhouseResult(
        results=[
            ("query1", "host1.internal", profile_map),
            ("query2", "host2.internal", {"SelectedRows": 2}),
        ],
        meta=[
            ("query_id", "String"),
            ("host", "String"),
            ("ProfileEvents", "Map(String, UInt64)"),
        ],
        profile=ClickhouseProfile(bytes=0, progress_bytes=0, blocks=0, rows=2, elapsed=0.1),
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

    sql = mock_connection.execute.call_args.kwargs["query"]
    assert "SELECT" in sql
    assert "hostname() AS host" in sql
    assert "ProfileEvents" in sql
    assert "query_id IN ('query1')" in sql
    assert "now() - INTERVAL 5 MINUTE" in sql

    assert trace_output.profile_events_meta == [[("ProfileEvents", "Map(String, UInt64)")]]
    assert trace_output.profile_events_profile == {
        "bytes": 0,
        "progress_bytes": 0,
        "blocks": 0,
        "rows": 2,
        "elapsed": 0.1,
    }
    # Keys use summary node_name when query_id maps; otherwise hostname().
    assert trace_output.profile_events_results == {
        "host1": {
            "column_names": ["ProfileEvents"],
            "rows": [json.dumps(profile_map)],
        },
        "host2.internal": {
            "column_names": ["ProfileEvents"],
            "rows": [json.dumps({"SelectedRows": 2})],
        },
    }


def test_gather_profile_events_appends_multiple_rows_per_host() -> None:
    trace_output = MagicMock()
    trace_output.summarized_trace_output.query_summaries = {
        "host1": MagicMock(query_id="query1"),
    }
    trace_output.query_id = "query1"
    trace_output.profile_events_meta = []
    trace_output.profile_events_results = {}

    mock_connection = MagicMock()
    mock_connection.execute.return_value = ClickhouseResult(
        results=[
            ("query1", "host1", {"SelectedRows": 1}),
            ("query1", "host1", {"SelectedRows": 2}),
        ],
        meta=[
            ("query_id", "String"),
            ("host", "String"),
            ("ProfileEvents", "Map(String, UInt64)"),
        ],
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

    assert trace_output.profile_events_results["host1"] == {
        "column_names": ["ProfileEvents"],
        "rows": [
            json.dumps({"SelectedRows": 1}),
            json.dumps({"SelectedRows": 2}),
        ],
    }


def test_gather_profile_events_escapes_query_ids() -> None:
    trace_output = MagicMock()
    # Attacker-controlled trace_logs can put quotes/SQL into the parsed query_id.
    malicious_id = "x' OR 1=1 --"
    trace_output.summarized_trace_output.query_summaries = {
        "host1": MagicMock(query_id=malicious_id),
    }
    trace_output.query_id = malicious_id
    trace_output.profile_events_meta = []
    trace_output.profile_events_results = {}

    mock_connection = MagicMock()
    mock_connection.execute.return_value = ClickhouseResult(results=[])

    with (
        patch(
            "snuba.admin.clickhouse.profile_events.get_ro_query_node_connection",
            return_value=mock_connection,
        ),
        patch(
            "snuba.admin.clickhouse.profile_events.system_log_source",
            return_value="system.query_log",
        ),
        patch("snuba.admin.clickhouse.tracing.time.sleep"),
    ):
        gather_profile_events(trace_output, "test_storage")

    sql = mock_connection.execute.call_args.kwargs["query"]
    assert "x' OR 1=1 --" not in sql
    assert r"'x\' OR 1=1 --'" in sql


def test_gather_profile_events_waits_for_all_query_ids() -> None:
    trace_output = MagicMock()
    trace_output.summarized_trace_output.query_summaries = {
        "host1": MagicMock(query_id="query1"),
        "host2": MagicMock(query_id="query2"),
    }
    trace_output.query_id = "query1"
    trace_output.profile_events_meta = []
    trace_output.profile_events_results = {}

    partial_result = ClickhouseResult(
        results=[("query1", "host1", {"SelectedRows": 1})],
        meta=[
            ("query_id", "String"),
            ("host", "String"),
            ("ProfileEvents", "Map(String, UInt64)"),
        ],
    )
    complete_result = ClickhouseResult(
        results=[
            ("query1", "host1", {"SelectedRows": 1}),
            ("query2", "host2", {"SelectedRows": 2}),
        ],
        meta=[
            ("query_id", "String"),
            ("host", "String"),
            ("ProfileEvents", "Map(String, UInt64)"),
        ],
        profile=ClickhouseProfile(bytes=0, progress_bytes=0, blocks=0, rows=2, elapsed=0.1),
    )

    mock_connection = MagicMock()
    mock_connection.execute.side_effect = [partial_result, complete_result]

    with (
        patch(
            "snuba.admin.clickhouse.profile_events.get_ro_query_node_connection",
            return_value=mock_connection,
        ),
        patch(
            "snuba.admin.clickhouse.profile_events.system_log_source",
            return_value="system.query_log",
        ),
        patch("snuba.admin.clickhouse.tracing.time.sleep") as mock_sleep,
    ):
        gather_profile_events(trace_output, "test_storage")

    assert mock_connection.execute.call_count == 2
    assert mock_sleep.call_count == 1
    assert set(trace_output.profile_events_results) == {"host1", "host2"}
    assert trace_output.profile_events_results["host1"]["rows"] == [json.dumps({"SelectedRows": 1})]
    assert trace_output.profile_events_results["host2"]["rows"] == [json.dumps({"SelectedRows": 2})]
