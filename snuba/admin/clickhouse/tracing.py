from __future__ import annotations

import math
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any, cast
from uuid import UUID

import structlog

from snuba.admin.clickhouse.common import (
    get_ro_query_node_connection,
    validate_ro_query,
)
from snuba.admin.clickhouse.trace_log_parsing import (
    ExecuteSummary,
    QuerySummary,
    TracingSummary,
    summarize_trace_output,
)
from snuba.clickhouse.native import ClickhousePool, ClickhouseResult
from snuba.clusters.cluster import ClickhouseClientSettings
from snuba.datasets.storages.factory import get_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.utils.constants import (
    PROFILE_EVENTS_MAX_ATTEMPTS,
    PROFILE_EVENTS_MAX_WAIT_SECONDS,
)

logger = structlog.get_logger().bind(module=__name__)


@dataclass
class TraceOutput:
    trace_output: str
    summarized_trace_output: TracingSummary
    cols: list[tuple[str, str]]
    num_rows_result: int
    result: list[tuple[Any, ...]]
    profile_events_results: dict[str, Any]
    profile_events_meta: list[Any]
    profile_events_profile: dict[str, int]
    query_id: str = ""


def run_query_and_get_trace(
    storage_name: str, query: str, settings: Mapping[str, Any] | None = None
) -> TraceOutput:
    validate_ro_query(query)
    connection = get_ro_query_node_connection(storage_name, ClickhouseClientSettings.TRACING)
    query_result = connection.execute(
        query=query,
        capture_trace=True,
        with_column_types=True,
        settings=settings or {},
    )

    trace_output = query_result.trace_output or ""
    summarized_trace_output = summarize_trace_output(trace_output)
    query_id = _resolve_query_id(query_result, summarized_trace_output)

    if query_id:
        summarized_trace_output = merge_query_log_summary(
            summarized_trace_output,
            summarize_from_query_log(connection, storage_name, query_id),
        )

    if not trace_output.strip() and summarized_trace_output.query_summaries:
        trace_output = format_trace_output_from_summary(summarized_trace_output)

    return TraceOutput(
        trace_output=trace_output,
        summarized_trace_output=summarized_trace_output,
        cols=cast("list[tuple[str, str]]", query_result.meta),
        num_rows_result=len(query_result.results),
        result=list(map(scrub_row, query_result.results)),
        profile_events_results={},
        profile_events_meta=[],
        profile_events_profile={},
        query_id=query_id,
    )


def _resolve_query_id(
    query_result: ClickhouseResult, summarized_trace_output: TracingSummary
) -> str:
    if query_result.query_id:
        return query_result.query_id

    for summary in summarized_trace_output.query_summaries.values():
        if summary.is_distributed and summary.query_id:
            return summary.query_id

    if summarized_trace_output.query_summaries:
        return next(iter(summarized_trace_output.query_summaries.values())).query_id

    return ""


def system_log_source(storage_name: str, table: str) -> str:
    try:
        cluster = get_storage(StorageKey(storage_name)).get_cluster()
        cluster_name = cluster.get_clickhouse_cluster_name()
        if cluster_name and not cluster.is_single_node():
            return f"clusterAllReplicas('{cluster_name}', system.{table})"
    except Exception:
        logger.warning(
            "Could not resolve cluster for system log source; using local table",
            storage=storage_name,
            table=table,
            exc_info=True,
        )
    return f"system.{table}"


def poll_system_query(
    connection: ClickhousePool,
    sql: str,
    *,
    accept_result: Callable[[ClickhouseResult], bool] | None = None,
) -> ClickhouseResult | None:
    is_acceptable = accept_result or (lambda result: bool(result and result.results))
    wait_time = 1
    last_result: ClickhouseResult | None = None
    for attempt in range(PROFILE_EVENTS_MAX_ATTEMPTS):
        try:
            last_result = connection.execute(query=sql, with_column_types=True)
        except Exception:
            logger.warning(
                "System log poll failed",
                attempt=attempt,
                exc_info=True,
            )
            last_result = None

        if last_result is not None and is_acceptable(last_result):
            return last_result

        if attempt + 1 < PROFILE_EVENTS_MAX_ATTEMPTS:
            wait_time = min(wait_time * 2, PROFILE_EVENTS_MAX_WAIT_SECONDS)
            time.sleep(wait_time)

    return last_result


def summarize_from_query_log(
    connection: ClickhousePool,
    storage_name: str,
    query_id: str,
) -> TracingSummary:
    source = system_log_source(storage_name, "query_log")
    sql = f"""
        SELECT
            hostname() AS host,
            query_id,
            is_initial_query,
            read_rows,
            formatReadableSize(read_bytes) AS memory_size,
            query_duration_ms / 1000.0 AS seconds,
            if(query_duration_ms > 0, read_rows / (query_duration_ms / 1000.0), 0) AS rows_per_second,
            if(
                query_duration_ms > 0,
                formatReadableSize(read_bytes / (query_duration_ms / 1000.0)),
                '0.00 B'
            ) AS bytes_per_second
        FROM {source}
        WHERE event_time >= now() - INTERVAL 5 MINUTE
          AND type = 'QueryFinish'
          AND (query_id = '{query_id}' OR initial_query_id = '{query_id}')
        ORDER BY is_initial_query DESC, event_time
    """

    def _root_finish_present(result: ClickhouseResult) -> bool:
        if not result.results:
            return False
        return any(bool(row[2]) for row in result.results if row)

    result = poll_system_query(connection, sql, accept_result=_root_finish_present)
    summary = TracingSummary({})
    if result is None or not result.results:
        return summary

    for row in result.results:
        (
            host,
            row_query_id,
            is_initial_query,
            read_rows,
            memory_size,
            seconds,
            rows_per_second,
            bytes_per_second,
        ) = row
        node_name = str(host)

        execute = ExecuteSummary(
            rows_read=int(read_rows or 0),
            memory_size=str(memory_size),
            seconds=float(seconds or 0),
            rows_per_second=float(rows_per_second or 0),
            bytes_per_second=str(bytes_per_second),
        )

        existing = summary.query_summaries.get(node_name)
        if existing is None:
            summary.query_summaries[node_name] = QuerySummary(
                node_name=node_name,
                is_distributed=bool(is_initial_query),
                query_id=str(row_query_id),
                execute_summaries=[execute],
            )
        else:
            if existing.execute_summaries is None:
                existing.execute_summaries = [execute]
            else:
                existing.execute_summaries.append(execute)

    return summary


def format_trace_output_from_summary(summary: TracingSummary) -> str:
    lines: list[str] = []
    nodes = sorted(
        summary.query_summaries.values(),
        key=lambda s: (not s.is_distributed, s.node_name),
    )
    for node in nodes:
        role = "Distributed" if node.is_distributed else "Local"
        if node.execute_summaries:
            for execute in node.execute_summaries:
                lines.append(
                    f"[ {node.node_name} ] {{{node.query_id}}} <Debug> executeQuery "
                    f"({role}): Read {execute.rows_read} rows, {execute.memory_size} in "
                    f"{execute.seconds} sec., {execute.rows_per_second} rows/sec., "
                    f"{execute.bytes_per_second}/sec."
                )
        else:
            lines.append(f"[ {node.node_name} ] {{{node.query_id}}} <Debug> executeQuery ({role})")
    return "\n".join(lines)


def merge_query_log_summary(base: TracingSummary, from_query_log: TracingSummary) -> TracingSummary:
    if not from_query_log.query_summaries:
        return base
    if not base.query_summaries:
        return from_query_log

    merged = TracingSummary(dict(base.query_summaries))
    for node_name, log_summary in from_query_log.query_summaries.items():
        existing = merged.query_summaries.get(node_name)
        if existing is None:
            merged.query_summaries[node_name] = log_summary
            continue
        if not existing.execute_summaries and log_summary.execute_summaries:
            existing.execute_summaries = list(log_summary.execute_summaries)
        # Only overwrite flags for nodes query_log actually returned. Missing
        # hosts may be a hostname mismatch, not proof the node is non-distributed.
        existing.is_distributed = log_summary.is_distributed

    return merged


def is_hex(value: Any) -> bool:
    if not isinstance(value, str):
        return False

    try:
        int(value, 16)
        return True
    except Exception:
        return False


def scrub_row(row: tuple[Any, ...]) -> tuple[Any, ...]:
    rv: list[Any] = []
    for val in row:
        if isinstance(val, (datetime, UUID)) or is_hex(val):
            rv.append(val)
        elif isinstance(val, (int, float)):
            if math.isnan(val):
                rv.append(None)
            else:
                rv.append(val)
        else:
            rv.append(f"<scrubbed: {type(val).__name__}>")

    return tuple(rv)
