from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Mapping

from snuba.admin.clickhouse.common import (
    get_ro_query_node_connection,
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
    connection = get_ro_query_node_connection(
        storage_name, ClickhouseClientSettings.TRACING
    )
    query_result = connection.execute(
        query=query, capture_trace=True, with_column_types=True, settings=settings or {}
    )
    summarized_trace_output = summarize_trace_output(query_result.trace_output)
    return TraceOutput(
        trace_output=query_result.trace_output,
        summarized_trace_output=summarized_trace_output,
        cols=query_result.meta,  # type: ignore
        num_rows_result=len(query_result.results),
        result=list(map(scrub_row, query_result.results)),
        profile_events_results={},
        profile_events_meta=[],
        profile_events_profile={},
    )


def scrub_row(row: tuple[Any, ...]) -> tuple[Any, ...]:
    rv: list[Any] = []
    for val in row:
        if isinstance(val, (int, float)):
            if math.isnan(val):
                rv.append(None)
            else:
                rv.append(val)
        else:
            rv.append(f"<scrubbed: {type(val).__name__}>")

    return tuple(rv)
