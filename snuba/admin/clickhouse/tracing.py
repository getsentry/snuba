from __future__ import annotations

import math
import time
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any, cast
from uuid import UUID, uuid4

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
class QueryTraceData:
    host: str
    port: int
    query_id: str
    node_name: str


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
    # Stable query id used for the traced statement. Populated so profile-event
    # collection can still find the query when the HTTP driver leaves
    # ``trace_output`` empty (no parseable node/query-id map).
    query_id: str = ""


def run_query_and_get_trace(
    storage_name: str, query: str, settings: Mapping[str, Any] | None = None
) -> TraceOutput:
    validate_ro_query(query)
    connection = get_ro_query_node_connection(storage_name, ClickhouseClientSettings.TRACING)
    # Always assign a query_id so we can recover performance data from
    # system.query_log / system.text_log when the driver cannot surface the
    # server's send_logs_level stream (clickhouse-connect/HTTP).
    query_id = str(uuid4())
    query_result = connection.execute(
        query=query,
        capture_trace=True,
        with_column_types=True,
        settings=settings or {},
        query_id=query_id,
    )

    trace_output = query_result.trace_output or ""
    if not trace_output.strip():
        # HTTP path: reconstruct whatever the server persisted for this query_id.
        trace_output = reconstruct_trace_from_system_logs(connection, storage_name, query_id)

    summarized_trace_output = summarize_trace_output(trace_output)
    if not summarized_trace_output.query_summaries:
        # Even if text_log was empty/disabled, query_log usually has the finish
        # row once logs flush. Build a minimal summary from that.
        summarized_trace_output = summarize_from_query_log(connection, storage_name, query_id)

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


def _system_log_source(storage_name: str, table: str) -> str:
    """
    Prefer clusterAllReplicas so distributed child queries on storage nodes are
    included. Fall back to the local system table when the storage is single-node
    or has no cluster name configured.
    """
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


def _poll_system_query(
    connection: ClickhousePool,
    sql: str,
) -> ClickhouseResult | None:
    """Poll a system-table query until it returns rows or attempts are exhausted."""
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

        if last_result is not None and last_result.results:
            return last_result

        if attempt + 1 < PROFILE_EVENTS_MAX_ATTEMPTS:
            time.sleep(min(wait_time, PROFILE_EVENTS_MAX_WAIT_SECONDS))
            wait_time *= 2

    return last_result


def _format_bytes(num_bytes: int | float) -> str:
    units = ["B", "KiB", "MiB", "GiB", "TiB"]
    value = float(num_bytes)
    for unit in units:
        if abs(value) < 1024.0 or unit == units[-1]:
            if unit == "B":
                return f"{int(value)} {unit}"
            return f"{value:.2f} {unit}"
        value /= 1024.0
    return f"{num_bytes} B"


def _level_name(level: Any) -> str:
    if isinstance(level, str):
        # Enum string form is often 'Debug' or '3'.
        return level.split(".")[-1] if level[:1].isdigit() is False else level
    try:
        # ClickHouse text_log level Enum8 values.
        mapping = {
            1: "Fatal",
            2: "Critical",
            3: "Error",
            4: "Warning",
            5: "Notice",
            6: "Information",
            7: "Debug",
            8: "Trace",
            9: "Test",
        }
        return mapping.get(int(level), str(level))
    except (TypeError, ValueError):
        return str(level)


def _related_query_ids(
    connection: ClickhousePool,
    storage_name: str,
    query_id: str,
) -> list[str]:
    """Return the root query_id plus any distributed child ids from query_log."""
    source = _system_log_source(storage_name, "query_log")
    sql = f"""
        SELECT DISTINCT query_id
        FROM {source}
        WHERE event_date >= yesterday()
          AND (query_id = '{query_id}' OR initial_query_id = '{query_id}')
    """
    result = _poll_system_query(connection, sql)
    if result is None or not result.results:
        return [query_id]
    ids = [str(row[0]) for row in result.results if row and row[0]]
    return ids or [query_id]


def reconstruct_trace_from_system_logs(
    connection: ClickhousePool,
    storage_name: str,
    query_id: str,
) -> str:
    """
    Rebuild the native-driver style multi-line trace string from system.text_log.

    clickhouse-connect cannot capture send_logs_level output on the wire, but
    when text_log is enabled the same lines are persisted server-side and can be
    pulled back by query_id (including distributed children via initial_query_id).
    """
    related_ids = _related_query_ids(connection, storage_name, query_id)
    id_list = ", ".join(f"'{qid}'" for qid in related_ids)
    source = _system_log_source(storage_name, "text_log")
    # Bound by event_date so ClickHouse can prune parts; tracing is interactive
    # so "today and yesterday" is plenty of headroom across midnight.
    sql = f"""
        SELECT
            hostname() AS host,
            thread_id,
            query_id,
            level,
            logger_name,
            message
        FROM {source}
        WHERE event_date >= yesterday()
          AND query_id IN ({id_list})
        ORDER BY event_time, microseconds
    """
    result = _poll_system_query(connection, sql)
    if result is None or not result.results:
        return ""

    lines: list[str] = []
    for row in result.results:
        host, thread_id, row_query_id, level, logger_name, message = row
        lines.append(
            f"[ {host} ] [ {thread_id} ] {{{row_query_id}}} "
            f"<{_level_name(level)}> {logger_name}: {message}"
        )
    return "\n".join(lines)


def summarize_from_query_log(
    connection: ClickhousePool,
    storage_name: str,
    query_id: str,
) -> TracingSummary:
    """
    Build a TracingSummary from system.query_log finish rows.

    This is the fallback when neither the native log stream nor text_log is
    available. query_log still carries duration/rows/bytes per node, which is
    enough for the formatted "Total" section in the tracing UI.
    """
    source = _system_log_source(storage_name, "query_log")
    sql = f"""
        SELECT
            hostname() AS host,
            query_id,
            is_initial_query,
            read_rows,
            read_bytes,
            query_duration_ms
        FROM {source}
        WHERE event_date >= yesterday()
          AND type = 'QueryFinish'
          AND (query_id = '{query_id}' OR initial_query_id = '{query_id}')
        ORDER BY is_initial_query DESC, event_time
    """
    result = _poll_system_query(connection, sql)
    summary = TracingSummary({})
    if result is None or not result.results:
        return summary

    for row in result.results:
        host, row_query_id, is_initial_query, read_rows, read_bytes, duration_ms = row
        node_name = str(host)
        seconds = float(duration_ms or 0) / 1000.0
        rows = int(read_rows or 0)
        nbytes = int(read_bytes or 0)
        rows_per_second = (rows / seconds) if seconds > 0 else 0.0
        bytes_per_second = _format_bytes(nbytes / seconds) if seconds > 0 else "0 B"

        execute = ExecuteSummary(
            rows_read=rows,
            memory_size=_format_bytes(nbytes),
            seconds=seconds,
            rows_per_second=rows_per_second,
            bytes_per_second=bytes_per_second,
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
