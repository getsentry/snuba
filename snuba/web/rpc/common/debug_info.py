from __future__ import annotations

import re
import textwrap
from typing import List

from sentry_protos.snuba.v1.request_common_pb2 import (
    QueryInfo,
    QueryMetadata,
    QueryStats,
    ResponseMeta,
    TimingMarks,
)

from snuba.query.query_settings import HTTPQuerySettings
from snuba.utils.metrics.timer import Timer
from snuba.web import QueryResult

COLOR_HOST = "\033[36m"  # Cyan
COLOR_THREAD_ID = "\033[33m"  # Yellow
COLOR_SESSION = "\033[35m"  # Magenta
COLOR_DEBUG = "\033[32m"  # Green
COLOR_TRACE = "\033[34m"  # Blue
COLOR_MESSAGE = "\033[37m"  # White
RESET = "\033[0m"

LOG_PATTERNS = {
    "host": re.compile(r"\[ (.*?) \]"),
    "thread_id": re.compile(r"\[ (\d+) \]"),
    "session": re.compile(r"\{(.*?)\}"),
    "level": re.compile(r"<(\w+)>"),
    "message": re.compile(r"> (.*)"),
}


def extract_response_meta(
    request_id: str,
    debug: bool,
    query_results: List[QueryResult],
    timers: List[Timer],
) -> ResponseMeta:
    """
    Extract metadata from query results for response.

    Args:
        request_id: Unique identifier for the request
        debug: Whether debug mode is enabled
        query_results: List of query results
        timers: List of timing information

    Returns:
        ResponseMeta object containing query metadata
    """
    query_info: List[QueryInfo] = []

    if not debug:
        return ResponseMeta(request_id=request_id, query_info=query_info)

    for query_result, timer in zip(query_results, timers):
        extra = getattr(query_result, "extra", None) or {}
        stats = extra.get("stats", {}) if isinstance(extra, dict) else {}
        result = getattr(query_result, "result", None) or {}
        profile = result.get("profile", {}) if isinstance(result, dict) else {}
        timer_data = timer.for_json()

        timing_marks = TimingMarks(
            marks_ms=timer_data.get("marks_ms", {}),
            duration_ms=int(timer_data.get("duration_ms", 0)),
            timestamp=int(timer_data.get("timestamp", 0)),
            tags=timer_data.get("tags", {}),
        )

        query_stats = QueryStats(
            rows_read=stats.get("result_rows", 0),
            columns_read=stats.get("result_cols", 0),
            blocks=profile.get("blocks", 0),
            progress_bytes=profile.get("progress_bytes", 0),
            max_threads=stats.get("quota_allowance", {})
            .get("summary", {})
            .get("threads_used"),
            timing_marks=timing_marks,
        )

        query_metadata = QueryMetadata(
            sql=extra.get("sql", ""),
            status=(
                "success"
                if stats.get("quota_allowance", {})
                .get("summary", {})
                .get("is_successful")
                else "failure"
            ),
            clickhouse_table=stats.get("clickhouse_table", ""),
            final=stats.get("final", False),
            query_id=stats.get("query_id", ""),
            consistent=stats.get("consistent", False),
            cache_hit=stats.get("cache_hit", False),
            cluster_name=stats.get("cluster_name", ""),
        )

        trace_logs = result.get("trace_output", "")
        query_info.append(
            QueryInfo(stats=query_stats, metadata=query_metadata, trace_logs=trace_logs)
        )

    return ResponseMeta(request_id=request_id, query_info=query_info)


def setup_trace_query_settings() -> HTTPQuerySettings:
    """
    Configure query settings for tracing.

    Returns:
        HTTPQuerySettings configured for tracing
    """
    query_settings = HTTPQuerySettings()
    query_settings.set_clickhouse_settings(
        {"send_logs_level": "trace", "log_profile_events": 1}
    )
    return query_settings


def format_trace_log(log: str, width: int = 140) -> str:
    """
    Format a ClickHouse trace log with colored output and proper wrapping.

    Args:
        log: Raw trace log string
        width: Maximum width for wrapped text lines

    Returns:
        Formatted string with ANSI colors and wrapped text
    """
    output = []
    for line in log.splitlines():
        if not line.strip():
            continue

        matches = {name: pattern.search(line) for name, pattern in LOG_PATTERNS.items()}

        if not all(matches.values()):
            continue

        host = matches["host"].group(1)
        thread_id = matches["thread_id"].group(1)
        session = matches["session"].group(1)
        level = matches["level"].group(1)
        message = matches["message"].group(1).strip()

        level_color = COLOR_DEBUG if level == "Debug" else COLOR_TRACE

        header = (
            f"{COLOR_HOST}[{host}]{RESET} "
            f"{COLOR_THREAD_ID}[{thread_id}]{RESET} "
            f"{COLOR_SESSION}{{{session}}}{RESET} "
            f"<{level_color}{level}{RESET}>"
        )

        wrapped_message = textwrap.fill(
            f"{COLOR_MESSAGE}{message}{RESET}", width=width, subsequent_indent=" " * 4
        )
        output.append(f"{header}\n{wrapped_message}")

    return "\n\n".join(output)
