from __future__ import annotations

import math
import time
from collections.abc import Callable, Mapping
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
    # system.query_log when the driver cannot surface the server's
    # send_logs_level stream (clickhouse-connect/HTTP). We do not rely on
    # system.text_log — it is not enabled in our environments.
    query_id = str(uuid4())
    query_result = connection.execute(
        query=query,
        capture_trace=True,
        with_column_types=True,
        settings=settings or {},
        query_id=query_id,
    )

    trace_output = query_result.trace_output or ""
    summarized_trace_output = summarize_trace_output(trace_output)

    # query_log is the durable source of per-node duration/rows/bytes. On the
    # HTTP driver the wire trace is empty, so this is the entire summary. On the
    # native driver it still fills missing execute totals and corrects the
    # distributed-node flag via is_initial_query.
    query_log_summary = summarize_from_query_log(connection, storage_name, query_id)
    summarized_trace_output = merge_query_log_summary(
        summarized_trace_output,
        query_log_summary,
    )

    if not trace_output.strip() and summarized_trace_output.query_summaries:
        # Raw UI mode still needs something to show when the driver left the
        # wire trace empty. Synthesize simple execute lines from query_log.
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
    *,
    accept_result: Callable[[ClickhouseResult], bool] | None = None,
) -> ClickhouseResult | None:
    """
    Poll a system-table query until it returns an acceptable result or attempts
    are exhausted.

    ``accept_result``, when provided, is called with the latest ClickhouseResult
    and should return True when polling can stop. Defaults to "any non-empty
    result set".
    """
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


def summarize_from_query_log(
    connection: ClickhousePool,
    storage_name: str,
    query_id: str,
) -> TracingSummary:
    """
    Build a TracingSummary from system.query_log finish rows.

    This is the primary recovery path when the clickhouse-connect (HTTP) driver
    leaves the wire trace empty. query_log carries duration/rows/bytes per node
    for the formatted "Total" section, including distributed children via
    initial_query_id.
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

    def _root_finish_present(result: ClickhouseResult) -> bool:
        if not result.results:
            return False
        # Wait for the initiator finish row so we don't stop on a single early
        # shard finish while the root (or other shards) are still flushing.
        return any(bool(row[2]) for row in result.results if row)

    result = _poll_system_query(connection, sql, accept_result=_root_finish_present)
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


def format_trace_output_from_summary(summary: TracingSummary) -> str:
    """
    Build a simple multi-line trace string from query_log-derived summaries so
    the raw tracing UI is not blank when the HTTP driver has no wire logs.
    """
    lines: list[str] = []
    # Stable order: distributed initiator first, then others by name.
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
    """
    Merge query_log-derived node summaries into a (possibly partial) base summary.

    - Nodes only present in query_log are added.
    - Nodes already present keep richer native-driver wire-trace detail, but gain
      an execute_summary from query_log when they don't already have one.
    - ``is_distributed`` is taken from query_log when available. Wire-trace
      parsing marks whichever node appeared first as distributed; query_log's
      ``is_initial_query`` is authoritative.
    """
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
        existing.is_distributed = log_summary.is_distributed

    query_log_has_distributed = any(
        summary.is_distributed for summary in from_query_log.query_summaries.values()
    )
    if query_log_has_distributed:
        for node_name, summary in merged.query_summaries.items():
            query_log_node = from_query_log.query_summaries.get(node_name)
            if query_log_node is not None:
                summary.is_distributed = query_log_node.is_distributed
            else:
                # Node only seen in the wire trace; if another node is the
                # confirmed initiator, this one is not distributed.
                summary.is_distributed = False

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
