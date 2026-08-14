from unittest.mock import MagicMock, patch

from snuba.admin.clickhouse.trace_log_parsing import ExecuteSummary, QuerySummary, TracingSummary
from snuba.admin.clickhouse.tracing import (
    MAX_TRACING_QUERY_LIMIT,
    _extract_settings_clause,
    format_trace_output_from_summary,
    merge_query_log_summary,
    run_query_and_get_trace,
    scrub_row,
    summarize_from_query_log,
)
from snuba.clickhouse.pool import ClickhouseResult


def test_scrub() -> None:
    assert scrub_row((1, 2, 3, "release name")) == (1, 2, 3, "<scrubbed: str>")


def test_extract_settings_clause_moves_settings() -> None:
    body, settings, apply_limit = _extract_settings_clause(
        "SELECT 1 SETTINGS max_threads = 10, distributed_product_mode = 'global'"
    )
    assert body == "SELECT 1"
    assert settings == {"max_threads": "10", "distributed_product_mode": "global"}
    assert apply_limit is True

    body, settings, apply_limit = _extract_settings_clause("SELECT 1")
    assert body == "SELECT 1"
    assert settings == {}
    assert apply_limit is True

    # Malformed trailing SETTINGS: leave SQL alone and disable driver LIMIT append.
    body, settings, apply_limit = _extract_settings_clause("SELECT 1 SETTINGS max_threads")
    assert body == "SELECT 1 SETTINGS max_threads"
    assert settings == {}
    assert apply_limit is False


def test_summarize_from_query_log() -> None:
    connection = MagicMock()
    # Columns already formatted by ClickHouse (formatReadableSize / duration math),
    # matching ExecuteSummary fields parsed from native wire logs.
    connection.execute.return_value = ClickhouseResult(
        results=[
            (
                "query-node",
                "qid-1",
                1,
                100,
                "2.00 KiB",
                0.25,
                400.0,
                "8.00 KiB",
                "SELECT 1 LIMIT 10000",
            ),
            (
                "storage-node",
                "qid-2",
                0,
                80,
                "1.00 KiB",
                0.1,
                800.0,
                "10.00 KiB",
                "SELECT 1",
            ),
        ]
    )

    with (
        patch("snuba.admin.clickhouse.tracing.system_log_source", return_value="system.query_log"),
        patch("snuba.admin.clickhouse.tracing.time.sleep"),
    ):
        summary = summarize_from_query_log(connection, "errors_ro", "qid-1")

    sql = connection.execute.call_args.kwargs["query"]
    assert "formatReadableSize(read_bytes)" in sql
    assert "now() - INTERVAL 5 MINUTE" in sql
    assert "initial_query_id" in sql

    assert set(summary.query_summaries) == {"query-node", "storage-node"}
    dist = summary.query_summaries["query-node"]
    assert dist.is_distributed is True
    assert dist.query_id == "qid-1"
    assert dist.query == "SELECT 1 LIMIT 10000"
    assert dist.execute_summaries == [
        ExecuteSummary(
            rows_read=100,
            memory_size="2.00 KiB",
            seconds=0.25,
            rows_per_second=400.0,
            bytes_per_second="8.00 KiB",
        )
    ]
    local = summary.query_summaries["storage-node"]
    assert local.is_distributed is False
    assert local.execute_summaries is not None
    assert local.execute_summaries[0] == ExecuteSummary(
        rows_read=80,
        memory_size="1.00 KiB",
        seconds=0.1,
        rows_per_second=800.0,
        bytes_per_second="10.00 KiB",
    )


def test_format_trace_output_from_summary() -> None:
    summary = TracingSummary(
        {
            "storage-node": QuerySummary(
                node_name="storage-node",
                is_distributed=False,
                query_id="qid-2",
                execute_summaries=[
                    ExecuteSummary(
                        rows_read=80,
                        memory_size="1.00 KiB",
                        seconds=0.1,
                        rows_per_second=800.0,
                        bytes_per_second="10.00 KiB",
                    )
                ],
            ),
            "query-node": QuerySummary(
                node_name="query-node",
                is_distributed=True,
                query_id="qid-1",
                execute_summaries=[
                    ExecuteSummary(
                        rows_read=100,
                        memory_size="2.00 KiB",
                        seconds=0.25,
                        rows_per_second=400.0,
                        bytes_per_second="8.00 KiB",
                    )
                ],
            ),
        }
    )

    output = format_trace_output_from_summary(summary)
    lines = output.splitlines()
    # Same field order the UI / log parser use for execute lines.
    assert lines[0] == (
        "[ query-node ] {qid-1} <Debug> executeQuery (Distributed): "
        "Read 100 rows, 2.00 KiB in 0.25 sec., 400.0 rows/sec., 8.00 KiB/sec."
    )
    assert lines[1] == (
        "[ storage-node ] {qid-2} <Debug> executeQuery (Local): "
        "Read 80 rows, 1.00 KiB in 0.1 sec., 800.0 rows/sec., 10.00 KiB/sec."
    )


def test_merge_query_log_summary_overwrites_flags_only_for_shared_nodes() -> None:
    # Wire-trace first-line heuristic wrongly marked a storage node distributed.
    base = TracingSummary(
        {
            "storage-node": QuerySummary(
                node_name="storage-node",
                is_distributed=True,
                query_id="qid-2",
            ),
            "query-node": QuerySummary(
                node_name="query-node",
                is_distributed=False,
                query_id="qid-1",
            ),
            # Present only in the wire trace / under a different hostname key.
            "trace-only-node": QuerySummary(
                node_name="trace-only-node",
                is_distributed=True,
                query_id="qid-3",
            ),
        }
    )
    from_log = TracingSummary(
        {
            "query-node": QuerySummary(
                node_name="query-node",
                is_distributed=True,
                query_id="qid-1",
                execute_summaries=[
                    ExecuteSummary(
                        rows_read=10,
                        memory_size="1.00 KiB",
                        seconds=0.1,
                        rows_per_second=100.0,
                        bytes_per_second="10.00 KiB",
                    )
                ],
            ),
            "storage-node": QuerySummary(
                node_name="storage-node",
                is_distributed=False,
                query_id="qid-2",
                execute_summaries=[
                    ExecuteSummary(
                        rows_read=5,
                        memory_size="512 B",
                        seconds=0.05,
                        rows_per_second=100.0,
                        bytes_per_second="10.00 KiB",
                    )
                ],
            ),
        }
    )

    merged = merge_query_log_summary(base, from_log)
    assert merged.query_summaries["query-node"].is_distributed is True
    assert merged.query_summaries["storage-node"].is_distributed is False
    # Missing from query_log must not force-clear the existing flag.
    assert merged.query_summaries["trace-only-node"].is_distributed is True


def test_merge_query_log_summary_adds_missing_nodes_and_execute() -> None:
    base = TracingSummary(
        {
            "query-node": QuerySummary(
                node_name="query-node",
                is_distributed=True,
                query_id="qid-1",
            )
        }
    )
    from_log = TracingSummary(
        {
            "query-node": QuerySummary(
                node_name="query-node",
                is_distributed=True,
                query_id="qid-1",
                execute_summaries=[
                    ExecuteSummary(
                        rows_read=10,
                        memory_size="1.00 KiB",
                        seconds=0.1,
                        rows_per_second=100.0,
                        bytes_per_second="10.00 KiB",
                    )
                ],
            ),
            "storage-node": QuerySummary(
                node_name="storage-node",
                is_distributed=False,
                query_id="qid-2",
                execute_summaries=[
                    ExecuteSummary(
                        rows_read=5,
                        memory_size="512 B",
                        seconds=0.05,
                        rows_per_second=100.0,
                        bytes_per_second="10.00 KiB",
                    )
                ],
            ),
        }
    )

    merged = merge_query_log_summary(base, from_log)
    assert set(merged.query_summaries) == {"query-node", "storage-node"}
    assert merged.query_summaries["query-node"].execute_summaries is not None
    assert merged.query_summaries["query-node"].execute_summaries[0].rows_read == 10
    assert merged.query_summaries["storage-node"].query_id == "qid-2"


def test_summarize_from_query_log_waits_for_root_finish() -> None:
    connection = MagicMock()
    # First poll only has a shard finish (is_initial_query=0).
    # Second poll includes the root finish.
    connection.execute.side_effect = [
        ClickhouseResult(
            results=[
                (
                    "storage-node",
                    "qid-2",
                    0,
                    80,
                    "1.00 KiB",
                    0.1,
                    800.0,
                    "10.00 KiB",
                    "SELECT 1",
                )
            ]
        ),
        ClickhouseResult(
            results=[
                (
                    "query-node",
                    "qid-1",
                    1,
                    100,
                    "2.00 KiB",
                    0.25,
                    400.0,
                    "8.00 KiB",
                    "SELECT 1 LIMIT 10000",
                ),
                (
                    "storage-node",
                    "qid-2",
                    0,
                    80,
                    "1.00 KiB",
                    0.1,
                    800.0,
                    "10.00 KiB",
                    "SELECT 1",
                ),
            ]
        ),
    ]

    with (
        patch("snuba.admin.clickhouse.tracing.system_log_source", return_value="system.query_log"),
        patch("snuba.admin.clickhouse.tracing.time.sleep") as mock_sleep,
    ):
        summary = summarize_from_query_log(connection, "errors_ro", "qid-1")

    assert set(summary.query_summaries) == {"query-node", "storage-node"}
    assert connection.execute.call_count == 2
    assert mock_sleep.call_count == 1


def test_run_query_and_get_trace_uses_query_log_when_wire_trace_empty() -> None:
    # Driver/server assigned the id; tracing should not invent one.
    # query_limit marks this as a clickhouse-connect pool.
    connection = MagicMock()
    connection.query_limit = 0
    connection.execute.return_value = ClickhouseResult(
        results=[(1,)],
        meta=[("count()", "UInt64")],
        trace_output="",
        query_id="qid",
    )

    query_log_summary = TracingSummary(
        {
            "query-node": QuerySummary(
                node_name="query-node",
                is_distributed=True,
                query_id="qid",
                query="SELECT 1 LIMIT 10000",
                execute_summaries=[
                    ExecuteSummary(
                        rows_read=1,
                        memory_size="1 B",
                        seconds=0.001,
                        rows_per_second=1000.0,
                        bytes_per_second="1.00 KiB",
                    )
                ],
            ),
            "storage-node": QuerySummary(
                node_name="storage-node",
                is_distributed=False,
                query_id="child",
                execute_summaries=[
                    ExecuteSummary(
                        rows_read=5,
                        memory_size="512 B",
                        seconds=0.05,
                        rows_per_second=100.0,
                        bytes_per_second="10.00 KiB",
                    )
                ],
            ),
        }
    )

    with (
        patch(
            "snuba.admin.clickhouse.tracing.get_ro_query_node_connection",
            return_value=connection,
        ),
        patch("snuba.admin.clickhouse.tracing.validate_ro_query"),
        patch(
            "snuba.admin.clickhouse.tracing.summarize_from_query_log",
            return_value=query_log_summary,
        ) as mock_summary,
    ):
        output = run_query_and_get_trace(
            "errors_ro",
            "SELECT 1 SETTINGS max_threads = 10",
            settings={"log_profile_events": 1},
        )

    mock_summary.assert_called_once_with(connection, "errors_ro", "qid")
    assert output.query_id == "qid"
    assert set(output.summarized_trace_output.query_summaries) == {
        "query-node",
        "storage-node",
    }
    assert "[ query-node ] {qid} <Debug> executeQuery (Distributed):" in output.trace_output
    assert "[ storage-node ] {child} <Debug> executeQuery (Local):" in output.trace_output
    # No client-side query_id was forced onto the execute call.
    assert connection.execute.call_args.kwargs.get("query_id") in (None, "")
    assert connection.execute.call_args.kwargs["capture_trace"] is True
    assert connection.execute.call_args.kwargs["query"] == "SELECT 1"
    assert connection.execute.call_args.kwargs["settings"] == {
        "log_profile_events": 1,
        "max_threads": "10",
    }
    # query_limit is passed per execute; the shared pool attribute is untouched.
    assert connection.query_limit == 0
    assert connection.execute.call_args.kwargs["query_limit"] == MAX_TRACING_QUERY_LIMIT
    assert output.executed_query == "SELECT 1 LIMIT 10000"


def test_run_query_and_get_trace_disables_query_limit_when_settings_remain() -> None:
    connection = MagicMock()
    # Simulate a connect pool (has query_limit attr) without mutating it.
    connection.query_limit = MAX_TRACING_QUERY_LIMIT
    connection.execute.return_value = ClickhouseResult(
        results=[(1,)],
        meta=[("count()", "UInt64")],
        trace_output="",
        query_id="qid",
    )

    with (
        patch(
            "snuba.admin.clickhouse.tracing.get_ro_query_node_connection",
            return_value=connection,
        ),
        patch("snuba.admin.clickhouse.tracing.validate_ro_query"),
        patch(
            "snuba.admin.clickhouse.tracing.summarize_from_query_log",
            return_value=TracingSummary({}),
        ),
    ):
        run_query_and_get_trace("errors_ro", "SELECT 1 SETTINGS max_threads")

    # SETTINGS stayed in SQL, so this execute disables LIMIT without touching the pool.
    assert connection.query_limit == MAX_TRACING_QUERY_LIMIT
    assert connection.execute.call_args.kwargs["query"] == "SELECT 1 SETTINGS max_threads"
    assert connection.execute.call_args.kwargs["query_limit"] == 0


def test_run_query_and_get_trace_keeps_native_wire_trace() -> None:
    connection = MagicMock()
    wire_trace = (
        "[ query-node ] [ 1 ] {qid} <Debug> executeQuery: "
        "Read 1 rows, 1.00 B in 0.001 sec., 1000 rows/sec., 1.00 KiB/sec."
    )
    # Native driver did not surface a query_id; recover it from the wire trace.
    connection.execute.return_value = ClickhouseResult(
        results=[(1,)],
        meta=[("count()", "UInt64")],
        trace_output=wire_trace,
        query_id="",
    )

    with (
        patch(
            "snuba.admin.clickhouse.tracing.get_ro_query_node_connection",
            return_value=connection,
        ),
        patch("snuba.admin.clickhouse.tracing.validate_ro_query"),
        patch(
            "snuba.admin.clickhouse.tracing.summarize_from_query_log",
            return_value=TracingSummary({}),
        ) as mock_summary,
    ):
        output = run_query_and_get_trace("errors_ro", "SELECT 1")

    assert output.trace_output == wire_trace
    assert output.query_id == "qid"
    assert "query-node" in output.summarized_trace_output.query_summaries
    mock_summary.assert_called_once_with(connection, "errors_ro", "qid")
