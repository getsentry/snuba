from __future__ import annotations

import math
import re
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
from snuba.clickhouse.escaping import escape_string
from snuba.clickhouse.native import ClickhousePool, ClickhouseResult
from snuba.clusters.cluster import ClickhouseClientSettings
from snuba.datasets.storages.factory import get_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.utils.constants import (
    PROFILE_EVENTS_MAX_ATTEMPTS,
    PROFILE_EVENTS_MAX_WAIT_SECONDS,
)

logger = structlog.get_logger().bind(module=__name__)

MAX_TRACING_QUERY_LIMIT = 10_000

# Mirrors clickhouse-connect's limit detection so the executed-query display matches
# what the driver will do when query_limit is set.
_LIMIT_RE = re.compile(r"\s+LIMIT($|\s)", re.IGNORECASE)
_SETTINGS_ASSIGNMENT_RE = re.compile(
    r"([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.*)$",
)


def _find_top_level_settings_index(query: str) -> int | None:
    """Return the index of the first top-level SETTINGS keyword, if any."""
    depth = 0
    in_single = in_double = in_backtick = False
    escape = False
    index = 0
    length = len(query)

    while index < length:
        char = query[index]
        if in_single:
            if escape:
                escape = False
            elif char == "\\":
                escape = True
            elif char == "'":
                in_single = False
        elif in_double:
            if escape:
                escape = False
            elif char == "\\":
                escape = True
            elif char == '"':
                in_double = False
        elif in_backtick:
            if char == "`":
                in_backtick = False
        elif char == "'":
            in_single = True
        elif char == '"':
            in_double = True
        elif char == "`":
            in_backtick = True
        elif char == "(":
            depth += 1
        elif char == ")":
            depth = max(0, depth - 1)
        elif (
            depth == 0
            and query[index : index + 8].upper() == "SETTINGS"
            and (index == 0 or not query[index - 1].isalnum())
            and (index + 8 >= length or not query[index + 8].isalnum())
        ):
            return index
        index += 1
    return None


def _parse_settings_clause(clause: str) -> dict[str, Any]:
    """Parse `key = value, ...` pairs from a SETTINGS clause body."""
    parts: list[str] = []
    current: list[str] = []
    depth = 0
    in_single = False
    escape = False

    for char in clause:
        if in_single:
            current.append(char)
            if escape:
                escape = False
            elif char == "\\":
                escape = True
            elif char == "'":
                in_single = False
            continue

        if char == "'":
            in_single = True
            current.append(char)
            continue
        if char == "(":
            depth += 1
            current.append(char)
            continue
        if char == ")":
            depth = max(0, depth - 1)
            current.append(char)
            continue
        if char == "," and depth == 0:
            parts.append("".join(current).strip())
            current = []
            continue
        current.append(char)

    if current:
        parts.append("".join(current).strip())

    settings: dict[str, Any] = {}
    for part in parts:
        if not part:
            continue
        match = _SETTINGS_ASSIGNMENT_RE.match(part)
        if match is None:
            continue
        key, raw_value = match.group(1), match.group(2).strip()
        if (raw_value.startswith("'") and raw_value.endswith("'")) or (
            raw_value.startswith('"') and raw_value.endswith('"')
        ):
            settings[key] = raw_value[1:-1]
            continue
        try:
            settings[key] = float(raw_value) if "." in raw_value else int(raw_value)
        except ValueError:
            settings[key] = raw_value
    return settings


def _extract_settings_clause(query: str) -> tuple[str, dict[str, Any]]:
    """
    Move a top-level SETTINGS clause out of the SQL string.

    clickhouse-connect's query_limit appends `LIMIT N` at the end of the SQL. That
    is invalid when the query already ends with SETTINGS, so tracing lifts those
    settings into the driver settings dict instead.
    """
    settings_at = _find_top_level_settings_index(query)
    if settings_at is None:
        return query, {}

    body = query[:settings_at].rstrip()
    clause = query[settings_at + len("SETTINGS") :].strip()
    return body, _parse_settings_clause(clause)


def _executed_query_with_limit(query: str, max_limit: int = MAX_TRACING_QUERY_LIMIT) -> str:
    """Best-effort SQL the connect driver will send when query_limit is enabled."""
    if _LIMIT_RE.search(query):
        return query
    return f"{query.rstrip()}\n LIMIT {max_limit}"


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
    # SQL shape we expect ClickHouse to run after SETTINGS extraction + query_limit.
    executed_query: str = ""


def run_query_and_get_trace(
    storage_name: str, query: str, settings: Mapping[str, Any] | None = None
) -> TraceOutput:
    validate_ro_query(query)
    query_without_settings, sql_settings = _extract_settings_clause(query)

    execute_settings: dict[str, Any] = dict(settings or {})
    # SQL SETTINGS win over request defaults for the same key.
    execute_settings.update(sql_settings)

    connection = get_ro_query_node_connection(storage_name, ClickhouseClientSettings.TRACING)
    # Prefer clickhouse-connect's client-side query_limit. Native pools do not set
    # this instance attribute and are left alone.
    if "query_limit" in getattr(connection, "__dict__", {}):
        connection.query_limit = MAX_TRACING_QUERY_LIMIT

    executed_query = _executed_query_with_limit(query_without_settings)
    query_result = connection.execute(
        query=query_without_settings,
        capture_trace=True,
        with_column_types=True,
        settings=execute_settings,
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
        executed_query=executed_query,
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
          AND (query_id = {escape_string(query_id)} OR initial_query_id = {escape_string(query_id)})
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
