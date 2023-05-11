from __future__ import annotations

from dataclasses import dataclass

from snuba.admin.clickhouse.common import (
    get_ro_query_node_connection,
    validate_ro_query,
)
from snuba.clusters.cluster import ClickhouseClientSettings


@dataclass
class TraceOutput:
    trace_output: str
    cols: list[tuple[str, str]]
    num_rows_result: int


def run_query_and_get_trace(storage_name: str, query: str) -> TraceOutput:
    validate_ro_query(query)
    connection = get_ro_query_node_connection(
        storage_name, ClickhouseClientSettings.TRACING
    )
    query_result = connection.execute(
        query=query, capture_trace=True, with_column_types=True
    )
    return TraceOutput(
        trace_output=query_result.trace_output,
        cols=query_result.meta,  # type: ignore
        num_rows_result=len(query_result.results),
    )
