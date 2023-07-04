from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Dict, List

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


@dataclass
class FormattedTrace:
    query_node: QueryNodeTraceResult
    storage_nodes: List[StorageNodeTraceResult]


@dataclass
class NodeTraceResult:
    node_name: str
    thread_ids: set[str] = set()


@dataclass
class QueryNodeTraceResult(NodeTraceResult):
    number_of_storage_nodes_accessed: int = 0
    aggregation_performance: List[str] = []
    read_performance: List[str] = []
    memory_performance: List[str] = []


@dataclass
class StorageNodeTraceResult(NodeTraceResult):
    key_conditions: List[str] = []
    skip_indexes: List[str] = []
    filtering_algorithm: List[str] = []
    selected_parts_and_marks: List[str] = []
    aggregation_method: List[str] = []
    aggregation_performance: List[str] = []
    read_performance: List[str] = []
    memory_performance: List[str] = []


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


LOG_MAPPINGS_FOR_QUERY_NODES = [
    ("<Trace>", "Aggregator", "aggregation_performance"),
    ("<Debug>", "Aggregator", "aggregation_performance"),
    ("<Information>", "executeQuery", "read_performance"),
    ("<Debug>", "MemoryTracker", "memory_performance"),
]
"""
[ snuba-query-tiger-1-3 ] [ 25145 ] {0ab58551-b253-448d-9065-90c7e3457209} <Trace> Aggregator: Merging partially aggregated blocks (bucket    = -1).
"""
"""
[ snuba-query-tiger-1-3 ] [ 25145 ] {0ab58551-b253-448d-9065-90c7e3457209} <Debug> Aggregator: Merged partially aggregated blocks. 1
"""
"""
[ snuba-query-tiger-1-3 ] [ 20102 ] {0ab58551-b253-448d-9065-90c7e3457209} <Information> executeQuery: Read 26722 rows, 5.53 MiB in 0.411881385 sec., 64877     rows/sec., 13.43 MiB/sec.
"""
"""
[ snuba-query-tiger-1-3 ] [ 20102 ] {0ab58551-b253-448d-9065-90c7e3457209} <Debug> MemoryTracker: Peak memory usage (for query): 0.00 B.
"""


LOG_MAPPINGS_FOR_STORAGE_NODES = [
    ("<Debug>", "Key Condition", "key_conditions"),
    ("<Debug>", "index condition", "skip_indexes"),
    ("<Trace>", "(SelectExecutor): Used", "filtering_algorithm"),
    ("<Debug>", "granules", "selected_parts_and_marks"),
    ("<Debug>", "partition key", "selected_parts_and_marks"),
    ("<Trace>", "Aggregation method", "aggregation_method"),
    ("<Debug>", "AggregatingTransform", "aggregation_performance"),
    ("<Information>", "executeQuery", "read_performance"),
    ("<Debug>", "MemoryTracker", "memory_performance"),
]
"""
[ snuba-errors-tiger-mz-5-6 ] [ 394492 ] {36bdc269-25be-473b-9543-1c11245a3627} <Debug> MemoryTracker: Peak memory usage (for query): 0.00 B.
"""
"""
[ snuba-errors-tiger-mz-1-2 ] [ 2196694 ] {2016b587-69bc-4e33-b49c-8d2865c04212} <Debug> default.errors_local (d6814824-c5ab-41f5-9681-4824c5abf1f5)            (SelectExecutor): MinMax index condition: unknown, unknown, unknown, (column 0 in [1686047468, +inf)), and, (column 0 in (-inf, 1687363014]), and, unknown, and,    true, and, unknown, and, and, and, unknown, and
"""
"""
[ snuba-errors-tiger-mz-3-3 ] [ 2220394 ] {811a0733-ddfb-434b-a907-c3b7b3f015db} <Trace> default.errors_local (b9acfdaa-4328-4485-b9ac-fdaa4328b485)            (SelectExecutor): Used generic exclusion search over index for part 30-20230605_0_528634_979 with 40 steps
"""
"""
[ snuba-errors-tiger-mz-3-3 ] [ 2220394 ] {811a0733-ddfb-434b-a907-c3b7b3f015db} <Debug> default.errors_local (b9acfdaa-4328-4485-b9ac-fdaa4328b485)            (SelectExecutor): Index `bf_release` has dropped 45/49 granules.
[ snuba-errors-tiger-mz-3-3 ] [ 2220394 ] {811a0733-ddfb-434b-a907-c3b7b3f015db} <Debug> default.errors_local (b9acfdaa-4328-4485-b9ac-fdaa4328b485)            (SelectExecutor): Selected 63/142 parts by partition key, 4 parts by primary key, 49/1081141 marks by primary key, 4 marks to read from 4 ranges
"""
"""
[ snuba-errors-tiger-mz-2-6 ] [ 71589 ] {75628bc0-9e6e-476f-af9f-c6f28e110d3a} <Trace> Aggregator: Aggregation method: serialized
"""
"""
[ snuba-errors-tiger-mz-1-2 ] [ 2196694 ] {2016b587-69bc-4e33-b49c-8d2865c04212} <Information> executeQuery: Read 4001 rows, 893.49 KiB in 0.040332086 sec., 99201 rows/sec., 21.63 MiB/sec.
"""
"""
[ snuba-errors-tiger-mz-5-6 ] [ 394492 ] {36bdc269-25be-473b-9543-1c11245a3627} <Debug> MemoryTracker: Peak memory usage (for query): 0.00 B.
"""


def format_trace_output(raw_trace_logs: str) -> None:
    formatted_logs = format_log_to_dict(raw_trace_logs)
    lookup: dict[str, NodeTraceResult] = {}  # node_name: NodeTraceResult
    query_node_name = formatted_logs[0]["node_name"]
    lookup[query_node_name] = QueryNodeTraceResult(query_node_name)

    for log in formatted_logs:
        node_name = log["node_name"]
        if node_name not in lookup:
            lookup[node_name] = StorageNodeTraceResult(node_name)

        trace_result = lookup[node_name]
        assert isinstance(trace_result, NodeTraceResult)
        trace_result.thread_ids.add(log["thread_id"])

        if node_name == query_node_name:
            assert isinstance(trace_result, QueryNodeTraceResult)
            for log_type, search_str, trace_attr in LOG_MAPPINGS_FOR_QUERY_NODES:
                find_and_add_log_line(
                    log, getattr(trace_result, trace_attr), log_type, search_str
                )
        else:
            assert isinstance(trace_result, StorageNodeTraceResult)
            for log_type, search_str, trace_attr in LOG_MAPPINGS_FOR_STORAGE_NODES:
                find_and_add_log_line(
                    log, getattr(trace_result, trace_attr), log_type, search_str
                )
    print(lookup)


def find_and_add_log_line(
    log: Dict[str, Any], trace_result_data: List[str], log_type: str, search_str: str
) -> None:
    if log["log_type"] == log_type and search_str in log["log_content"]:
        trace_result_data.append(log["log_content"])


def format_log_to_dict(raw_trace_logs: str) -> List[dict[str, Any]]:
    # CLICKHOUSE TRACING LOG STRUCTURE: '[ NODE ] [ THREAD_ID ] {QUERY_ID} <LOG_TYPE> LOG_LINE'
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
            "log_content": re.split(r"<.*?>", line)[1].strip(),
        }
        formatted_logs.append(formatted_log)
    return formatted_logs
