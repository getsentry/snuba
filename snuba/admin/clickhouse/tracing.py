from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict

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
class TraceOutput:
    trace_output: str
    summarized_trace_output: TracingSummary
    cols: list[tuple[str, str]]
    num_rows_result: int
    profile_events_results: dict[dict[str, str], Any]
    profile_events_meta: list[Any]
    profile_events_profile: Dict[str, int]


def run_query_and_get_trace(storage_name: str, query: str) -> TraceOutput:
    validate_ro_query(query)
    connection = get_ro_query_node_connection(
        storage_name, ClickhouseClientSettings.TRACING
    )
    query_result = connection.execute(
        query=query, capture_trace=True, with_column_types=True
    )
    summarized_trace_output = summarize_trace_output(query_result.trace_output)
    return TraceOutput(
        trace_output=query_result.trace_output,
        summarized_trace_output=summarized_trace_output,
        cols=query_result.meta,  # type: ignore
        num_rows_result=len(query_result.results),
        profile_events_results={},
        profile_events_meta=[],
        profile_events_profile={},
    )
