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
    formatted_trace_output: Dict[str, Any]
    cols: list[tuple[str, str]]
    num_rows_result: int


@dataclass
class FormattedTrace:
    query_node: QueryNodeTraceResult
    storage_nodes: List[StorageNodeTraceResult]


class NodeTraceResult:
    def __init__(self, node_name: str) -> None:
        self.node_name: str = node_name
        self.thread_ids: List[str] = []
        self.threads_used: int = 0


class QueryNodeTraceResult(NodeTraceResult):
    def __init__(self, node_name: str) -> None:
        super(QueryNodeTraceResult, self).__init__(node_name)
        self.node_type = "query"
        self.number_of_storage_nodes_accessed: int = 0
        self.storage_nodes_accessed: List[str] = []
        self.aggregation_performance: List[str] = []
        self.read_performance: List[str] = []
        self.memory_performance: List[str] = []


@dataclass
class StorageNodeTraceResult(NodeTraceResult):
    def __init__(self, node_name: str) -> None:
        super(StorageNodeTraceResult, self).__init__(node_name)
        self.node_type = "storage"
        self.key_conditions: List[str] = []
        self.skip_indexes: List[str] = []
        self.filtering_algorithm: List[str] = []
        self.selected_parts_and_marks: List[str] = []
        self.aggregation_method: List[str] = []
        self.aggregation_performance: List[str] = []
        self.read_performance: List[str] = []
        self.memory_performance: List[str] = []


def run_query_and_get_trace(storage_name: str, query: str) -> TraceOutput:
    validate_ro_query(query)
    connection = get_ro_query_node_connection(
        storage_name, ClickhouseClientSettings.TRACING
    )
    query_result = connection.execute(
        query=query, capture_trace=True, with_column_types=True
    )
    formatted_trace_output = format_trace_output(query_result.trace_output)
    # import json

    # print(json.dumps(formatted_trace_output, indent=4))
    return TraceOutput(
        trace_output=query_result.trace_output,
        formatted_trace_output=formatted_trace_output,
        cols=query_result.meta,  # type: ignore
        num_rows_result=len(query_result.results),
    )


LOG_MAPPINGS_FOR_QUERY_NODES = [
    ("<Trace>", ["Aggregator: Merging"], "aggregation_performance"),
    ("<Debug>", ["Aggregator"], "aggregation_performance"),
    ("<Information>", ["executeQuery"], "read_performance"),
    ("<Debug>", ["MemoryTracker"], "memory_performance"),
]


LOG_MAPPINGS_FOR_STORAGE_NODES = [
    ("<Debug>", ["Key condition"], "key_conditions"),
    ("<Debug>", ["index condition"], "skip_indexes"),
    (
        "<Trace>",
        ["binary search", "generic exclusion"],
        "filtering_algorithm",
    ),  # TODO: make this generic
    ("<Debug>", ["granules"], "selected_parts_and_marks"),
    ("<Debug>", ["partition key"], "selected_parts_and_marks"),
    ("<Trace>", ["Aggregation method"], "aggregation_method"),
    ("<Debug>", ["AggregatingTransform"], "aggregation_performance"),
    ("<Information>", ["executeQuery"], "read_performance"),
    ("<Debug>", ["MemoryTracker"], "memory_performance"),
]


def format_trace_output(raw_trace_logs: str) -> Dict[str, Any]:
    with open("/Users/enochtang/code/snuba/test.txt", "r") as reader:
        raw_logs = reader.read()
        formatted_logs = format_log_to_dict(raw_logs)

        result: dict[str, Any] = {}  # node_name: NodeTraceResult

        query_node_name = formatted_logs[0]["node_name"]
        result[query_node_name] = QueryNodeTraceResult(query_node_name)
        query_node_trace_result = result[query_node_name]
        assert isinstance(query_node_trace_result, QueryNodeTraceResult)

        for log in formatted_logs:
            node_name = log["node_name"]
            if node_name not in result:
                result[node_name] = StorageNodeTraceResult(node_name)
                query_node_trace_result.storage_nodes_accessed.append(node_name)
                query_node_trace_result.number_of_storage_nodes_accessed += 1

            trace_result = result[node_name]
            assert isinstance(trace_result, NodeTraceResult)
            if log["thread_id"] not in trace_result.thread_ids:
                trace_result.thread_ids.append(log["thread_id"])
                trace_result.threads_used = len(trace_result.thread_ids)

            if node_name == query_node_name:
                assert isinstance(trace_result, QueryNodeTraceResult)
                for log_type, search_strs, trace_attr in LOG_MAPPINGS_FOR_QUERY_NODES:
                    find_and_add_log_line(
                        log, getattr(trace_result, trace_attr), log_type, search_strs
                    )
            else:
                assert isinstance(trace_result, StorageNodeTraceResult)
                for (
                    log_type,
                    search_strs,
                    trace_attr,
                ) in LOG_MAPPINGS_FOR_STORAGE_NODES:
                    find_and_add_log_line(
                        log, getattr(trace_result, trace_attr), log_type, search_strs
                    )
        for key, value in result.items():
            result[key] = vars(value)
        return result


def find_and_add_log_line(
    log: Dict[str, Any],
    trace_result_data: List[str],
    log_type: str,
    search_strs: List[str],
) -> None:
    for search_str in search_strs:
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

        try:
            formatted_log = {
                "node_name": context[0][2:-2],
                "thread_id": context[1][2:-2],
                "log_type": log_type,
                "log_content": re.split(r"<.*?>", line)[1].strip(),
            }
        except Exception:
            # Error parsing log line, continue.
            pass

        formatted_logs.append(formatted_log)
    return formatted_logs
