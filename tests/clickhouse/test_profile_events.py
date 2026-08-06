from unittest.mock import MagicMock, patch

from flask import g, json

from snuba.admin.clickhouse.profile_events import (
    gather_profile_events,
    hostname_resolves,
    parse_trace_for_query_ids,
)
from snuba.admin.clickhouse.tracing import QueryTraceData


def test_hostname_resolves() -> None:
    assert hostname_resolves("localhost")
    assert not hostname_resolves("invalid-hostname-that-doesnt-exist-123.local")


def test_parse_trace_for_query_ids() -> None:
    trace_output = MagicMock()
    trace_output.summarized_trace_output.query_summaries = {
        "host1": MagicMock(query_id="query1"),
        "host2": MagicMock(query_id="query2"),
    }
    trace_output.query_id = ""

    with (
        patch("snuba.admin.clickhouse.profile_events.hostname_resolves") as mock_resolve,
        patch(
            "snuba.admin.clickhouse.profile_events.use_clickhouse_connect_driver",
            return_value=False,
        ),
    ):
        mock_resolve.return_value = True
        result = parse_trace_for_query_ids(trace_output)

        assert len(result) == 2
        assert result[0] == QueryTraceData(
            host="host1", port=9000, query_id="query1", node_name="host1"
        )
        assert result[1] == QueryTraceData(
            host="host2", port=9000, query_id="query2", node_name="host2"
        )

        mock_resolve.return_value = False
        result = parse_trace_for_query_ids(trace_output)

        assert len(result) == 2
        assert result[0] == QueryTraceData(
            host="127.0.0.1", port=9000, query_id="query1", node_name="host1"
        )
        assert result[1] == QueryTraceData(
            host="127.0.0.1", port=9000, query_id="query2", node_name="host2"
        )


def test_parse_trace_for_query_ids_uses_http_default_when_connect_driver() -> None:
    """Unknown hosts fall back to the default for the active driver."""
    trace_output = MagicMock()
    trace_output.summarized_trace_output.query_summaries = {
        "host1": MagicMock(query_id="query1"),
    }
    trace_output.query_id = ""

    with (
        patch("snuba.admin.clickhouse.profile_events.hostname_resolves", return_value=True),
        patch(
            "snuba.admin.clickhouse.profile_events.use_clickhouse_connect_driver",
            return_value=True,
        ),
    ):
        result = parse_trace_for_query_ids(trace_output)

    assert result == [QueryTraceData(host="host1", port=8123, query_id="query1", node_name="host1")]


def test_parse_trace_for_query_ids_falls_back_to_query_node() -> None:
    """
    When summaries are empty (HTTP driver path) but we still know the root
    query_id, profile-event collection should target the storage query node.
    """
    trace_output = MagicMock()
    trace_output.summarized_trace_output.query_summaries = {}
    trace_output.query_id = "root-query-id"

    mock_node = MagicMock()
    mock_node.host_name = "query-host"
    mock_node.native_port = 9000
    mock_cluster = MagicMock()
    mock_cluster.get_query_node.return_value = mock_node
    mock_storage = MagicMock()
    mock_storage.get_cluster.return_value = mock_cluster

    with patch("snuba.admin.clickhouse.profile_events.get_storage", return_value=mock_storage):
        result = parse_trace_for_query_ids(trace_output, storage="errors_ro")

    assert result == [
        QueryTraceData(
            host="query-host",
            port=9000,
            query_id="root-query-id",
            node_name="query-host",
        )
    ]


def test_gather_profile_events() -> None:
    trace_output = MagicMock()
    trace_output.summarized_trace_output.query_summaries = {
        "host1": MagicMock(query_id="query1"),
    }
    trace_output.query_id = ""
    trace_output.profile_events_meta = []
    trace_output.profile_events_results = {}

    mock_system_query_result = MagicMock()
    mock_system_query_result.results = [("profile_events",)]
    mock_system_query_result.meta = [("column1", "type1")]
    mock_system_query_result.profile = {"profile_key": 123}

    with patch(
        "snuba.admin.clickhouse.profile_events.run_system_query_on_host_with_sql"
    ) as mock_query:
        mock_query.return_value = mock_system_query_result
        with patch("snuba.admin.clickhouse.profile_events.hostname_resolves", return_value=True):
            from flask import Flask

            app = Flask(__name__)
            with app.app_context():
                g.user = "test_user"
                gather_profile_events(trace_output, "test_storage")

                mock_query.assert_called_once_with(
                    "host1",
                    9000,
                    "test_storage",
                    "SELECT ProfileEvents FROM system.query_log WHERE query_id = 'query1' AND type = 'QueryFinish'",
                    False,
                    False,
                    "test_user",
                )

                assert trace_output.profile_events_meta == [mock_system_query_result.meta]
                assert trace_output.profile_events_profile == mock_system_query_result.profile
                assert trace_output.profile_events_results["host1"] == {
                    "column_names": ["column1"],
                    "rows": [json.dumps("profile_events")],
                }


def test_gather_profile_events_retry_logic() -> None:
    trace_output = MagicMock()
    trace_output.summarized_trace_output.query_summaries = {
        "host1": MagicMock(query_id="query1"),
    }
    trace_output.query_id = ""

    empty_result = MagicMock()
    empty_result.results = []

    success_result = MagicMock()
    success_result.results = [("profile_events",)]
    success_result.meta = [("column1", "type1")]
    success_result.profile = {"profile_key": 123}

    with patch(
        "snuba.admin.clickhouse.profile_events.run_system_query_on_host_with_sql"
    ) as mock_query:
        mock_query.side_effect = [empty_result, empty_result, success_result]
        with (
            patch("snuba.admin.clickhouse.profile_events.hostname_resolves", return_value=True),
            patch("time.sleep") as mock_sleep,
        ):
            from flask import Flask

            app = Flask(__name__)
            with app.app_context():
                g.user = "test_user"

                gather_profile_events(trace_output, "test_storage")

                assert mock_query.call_count == 3
                assert mock_sleep.call_count == 2

                assert mock_sleep.call_args_list[0][0][0] == 2
                assert mock_sleep.call_args_list[1][0][0] == 4
