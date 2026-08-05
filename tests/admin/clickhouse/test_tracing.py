from unittest.mock import MagicMock, patch

from snuba.admin.clickhouse.trace_log_parsing import ExecuteSummary
from snuba.admin.clickhouse.tracing import (
    _format_bytes,
    _level_name,
    reconstruct_trace_from_system_logs,
    run_query_and_get_trace,
    scrub_row,
    summarize_from_query_log,
)
from snuba.clickhouse.native import ClickhouseResult


def test_scrub() -> None:
    assert scrub_row((1, 2, 3, "release name")) == (1, 2, 3, "<scrubbed: str>")


def test_format_bytes() -> None:
    assert _format_bytes(512) == "512 B"
    assert _format_bytes(2048) == "2.00 KiB"
    assert _format_bytes(5 * 1024 * 1024) == "5.00 MiB"


def test_level_name() -> None:
    assert _level_name(7) == "Debug"
    assert _level_name(8) == "Trace"
    assert _level_name("Debug") == "Debug"


def test_reconstruct_trace_from_system_logs() -> None:
    connection = MagicMock()
    connection.execute.return_value = ClickhouseResult(
        results=[
            (
                "query-node",
                11,
                "qid-1",
                7,
                "executeQuery",
                "Read 10 rows, 1.00 KiB in 0.1 sec., 100 rows/sec., 10.00 KiB/sec.",
            ),
            (
                "storage-node",
                22,
                "qid-2",
                8,
                "SelectExecutor",
                "Selected 1/1 parts by partition key, 1 parts by primary key, 2/2 marks by primary key, 2 marks to read from 1 ranges",
            ),
        ]
    )

    with (
        patch("snuba.admin.clickhouse.tracing._system_log_source", return_value="system.text_log"),
        patch("snuba.admin.clickhouse.tracing.time.sleep"),
    ):
        output = reconstruct_trace_from_system_logs(connection, "errors_ro", "qid-1")

    assert "[ query-node ] [ 11 ] {qid-1} <Debug> executeQuery: Read 10 rows" in output
    assert "[ storage-node ] [ 22 ] {qid-2} <Trace> SelectExecutor: Selected 1/1 parts" in output


def test_summarize_from_query_log() -> None:
    connection = MagicMock()
    connection.execute.return_value = ClickhouseResult(
        results=[
            ("query-node", "qid-1", 1, 100, 2048, 250),
            ("storage-node", "qid-2", 0, 80, 1024, 100),
        ]
    )

    with (
        patch("snuba.admin.clickhouse.tracing._system_log_source", return_value="system.query_log"),
        patch("snuba.admin.clickhouse.tracing.time.sleep"),
    ):
        summary = summarize_from_query_log(connection, "errors_ro", "qid-1")

    assert set(summary.query_summaries) == {"query-node", "storage-node"}
    dist = summary.query_summaries["query-node"]
    assert dist.is_distributed is True
    assert dist.query_id == "qid-1"
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
    assert local.execute_summaries[0].rows_read == 80
    assert local.execute_summaries[0].seconds == 0.1


def test_run_query_and_get_trace_reconstructs_when_trace_empty() -> None:
    connection = MagicMock()
    connection.execute.return_value = ClickhouseResult(
        results=[(1,)],
        meta=[("count()", "UInt64")],
        trace_output="",
    )

    reconstructed = (
        "[ query-node ] [ 1 ] {qid} <Debug> executeQuery: "
        "Read 1 rows, 1.00 B in 0.001 sec., 1000 rows/sec., 1.00 KiB/sec."
    )

    with (
        patch(
            "snuba.admin.clickhouse.tracing.get_ro_query_node_connection",
            return_value=connection,
        ),
        patch("snuba.admin.clickhouse.tracing.validate_ro_query"),
        patch(
            "snuba.admin.clickhouse.tracing.reconstruct_trace_from_system_logs",
            return_value=reconstructed,
        ) as mock_reconstruct,
        patch(
            "snuba.admin.clickhouse.tracing.uuid4", return_value=MagicMock(__str__=lambda s: "qid")
        ),
    ):
        output = run_query_and_get_trace("errors_ro", "SELECT 1")

    mock_reconstruct.assert_called_once()
    assert output.trace_output == reconstructed
    assert output.query_id == "qid"
    assert "query-node" in output.summarized_trace_output.query_summaries
    assert connection.execute.call_args.kwargs["query_id"] == "qid"
    assert connection.execute.call_args.kwargs["capture_trace"] is True


def test_run_query_and_get_trace_falls_back_to_query_log_summary() -> None:
    connection = MagicMock()
    connection.execute.return_value = ClickhouseResult(
        results=[(1,)],
        meta=[("count()", "UInt64")],
        trace_output="",
    )

    fallback_summary = MagicMock()
    fallback_summary.query_summaries = {"query-node": MagicMock()}

    with (
        patch(
            "snuba.admin.clickhouse.tracing.get_ro_query_node_connection",
            return_value=connection,
        ),
        patch("snuba.admin.clickhouse.tracing.validate_ro_query"),
        patch(
            "snuba.admin.clickhouse.tracing.reconstruct_trace_from_system_logs",
            return_value="",
        ),
        patch(
            "snuba.admin.clickhouse.tracing.summarize_from_query_log",
            return_value=fallback_summary,
        ) as mock_summary,
        patch(
            "snuba.admin.clickhouse.tracing.uuid4", return_value=MagicMock(__str__=lambda s: "qid")
        ),
    ):
        output = run_query_and_get_trace("errors_ro", "SELECT 1")

    mock_summary.assert_called_once()
    assert output.summarized_trace_output is fallback_summary
    assert output.trace_output == ""
    assert output.query_id == "qid"
