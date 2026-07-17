from __future__ import annotations

import math
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any, cast
from uuid import UUID

from snuba.admin.clickhouse.common import (
    get_ro_query_node_connection,
    is_scrub_exempt_column,
    validate_ro_query,
)
from snuba.admin.clickhouse.trace_log_parsing import (
    TracingSummary,
    summarize_trace_output,
)
from snuba.clusters.cluster import ClickhouseClientSettings


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


def run_query_and_get_trace(
    storage_name: str, query: str, settings: Mapping[str, Any] | None = None
) -> TraceOutput:
    validate_ro_query(query)
    connection = get_ro_query_node_connection(storage_name, ClickhouseClientSettings.TRACING)
    query_result = connection.execute(
        query=query, capture_trace=True, with_column_types=True, settings=settings or {}
    )
    summarized_trace_output = summarize_trace_output(query_result.trace_output)
    cols = cast("list[tuple[str, str]]", query_result.meta)
    return TraceOutput(
        trace_output=query_result.trace_output,
        summarized_trace_output=summarized_trace_output,
        cols=cols,
        num_rows_result=len(query_result.results),
        result=[scrub_row(row, cols) for row in query_result.results],
        profile_events_results={},
        profile_events_meta=[],
        profile_events_profile={},
    )


def is_hex(value: Any) -> bool:
    if not isinstance(value, str):
        return False

    try:
        int(value, 16)
        return True
    except Exception:
        return False


def scrub_row(row: tuple[Any, ...], meta: list[tuple[str, str]] | None = None) -> tuple[Any, ...]:
    rv: list[Any] = []
    for i, val in enumerate(row):
        col_name = meta[i][0] if meta and i < len(meta) else None
        if is_scrub_exempt_column(col_name) or isinstance(val, (datetime, UUID)) or is_hex(val):
            rv.append(val)
        elif isinstance(val, (int, float)):
            if math.isnan(val):
                rv.append(None)
            else:
                rv.append(val)
        else:
            rv.append(f"<scrubbed: {type(val).__name__}>")

    return tuple(rv)
