from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, List

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
    format_trace_output(query_result.trace_output)
    return TraceOutput(
        trace_output=query_result.trace_output,
        cols=query_result.meta,  # type: ignore
        num_rows_result=len(query_result.results),
    )


def format_trace_output(raw_trace_logs: str) -> List[dict[str, Any]]:
    # LOG STRUCTURE: '[ NODE ] [ THREAD_ID ] {SHA?} <LOG_TYPE> LOG_LINE'
    formatted_logs = []
    for line in raw_trace_logs.splitlines():
        context = re.findall(r"\[.*?\]", line)
        log_type = ""
        log_type_regex = re.search(r"<.*?>", line)
        if log_type_regex is not None:
            log_type = log_type_regex.group()

        formatted_log = {
            "node": context[0][2:-2],
            "thread_id": context[1][2:-2],
            "log_type": log_type,
            "log_body": re.split(r"<.*?>", line)[1].strip(),
        }
        formatted_logs.append(formatted_log)
    return formatted_logs
