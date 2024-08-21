from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

# [ spans-clickhouse-1 ] [ 65011 ] {0.21246445055947638} <Debug> default.spans_optimized_v2_traces (aacb1a4f-32d0-49ea-8985-9c0d92a079ae) (SelectExecutor): Index `bf_attr_str_5` has dropped 0/2199 granules.
INDEX_MATCHER_RE = re.compile(
    r"(?P<table_name>.*) \(.*\) \(SelectExecutor\): Index `(?P<index_name>.*)` has dropped (?P<dropped_granules>\d+)/(?P<total_granules>\d+) granules."
)


@dataclass
class IndexSummary:
    table_name: str
    index_name: str
    dropped_granules: int
    total_granules: int

    @staticmethod
    def from_log(log_line: str) -> IndexSummary | None:
        match = INDEX_MATCHER_RE.match(log_line)
        if not match:
            return None

        return IndexSummary(
            table_name=match.group("table_name"),
            index_name=match.group("index_name"),
            dropped_granules=int(match.group("dropped_granules")),
            total_granules=int(match.group("total_granules")),
        )


# [ spans-clickhouse-1 ] [ 65011 ] {0.21246445055947638} <Debug> default.spans_optimized_v2_traces (aacb1a4f-32d0-49ea-8985-9c0d92a079ae) (SelectExecutor): Selected 4/4 parts by partition key, 4 parts by primary key, 2199/2199 marks by primary key, 2031 marks to read from 22 ranges
SELECT_MATCHER_RE = re.compile(
    r"(?P<table_name>.*) \(.*\) \(SelectExecutor\): Selected (?P<parts>\d+)/(?P<total_parts>\d+) parts by partition key, (?P<primary_parts>\d+) parts by primary key, (?P<selected_marks>\d+)/(?P<total_marks>\d+) marks by primary key, (?P<marks_to_read>\d+) marks to read from \d+ ranges"
)


@dataclass
class SelectSummary:
    table_name: str
    parts_selected_by_partition_key: int
    total_parts: int
    parts_selected_by_primary_key: int
    marks_selected_by_primary_key: int
    total_marks: int
    marks_to_read_from_ranges: int

    @staticmethod
    def from_log(log_line: str) -> SelectSummary | None:
        match = SELECT_MATCHER_RE.match(log_line)
        if not match:
            return None

        return SelectSummary(
            table_name=match.group("table_name"),
            parts_selected_by_partition_key=int(match.group("parts")),
            total_parts=int(match.group("total_parts")),
            parts_selected_by_primary_key=int(match.group("primary_parts")),
            marks_selected_by_primary_key=int(match.group("selected_marks")),
            total_marks=int(match.group("total_marks")),
            marks_to_read_from_ranges=int(match.group("marks_to_read")),
        )


# [ spans-clickhouse-1 ] [ 65011 ] {0.21246445055947638} <Debug> default.spans_optimized_v2_traces (aacb1a4f-32d0-49ea-8985-9c0d92a079ae) (SelectExecutor): Reading approx. 8286299 rows with 4 streams
STREAM_MATCHER_RE = re.compile(
    r"(?P<table_name>.*) \(.*\) \(SelectExecutor\): Reading approx. (?P<approximate_rows>\d+) rows with (?P<streams>\d+) streams"
)


@dataclass
class StreamSummary:
    table_name: str
    approximate_rows: int
    streams: int

    @staticmethod
    def from_log(log_line: str) -> StreamSummary | None:
        match = STREAM_MATCHER_RE.match(log_line)
        if not match:
            return None

        return StreamSummary(
            table_name=match.group("table_name"),
            approximate_rows=int(match.group("approximate_rows")),
            streams=int(match.group("streams")),
        )


# [ snuba-st-1-2.c.mattrobenolt-kube.internal ] [ 848231 ] {f3fb112a-583f-4125-a424-bd1d21b6ecf2} <Trace> AggregatingTransform: Aggregated. 1 to 1 rows (from 17.00 B) in 0.024679052 sec. (40.520 rows/sec., 688.84 B/sec.)
AGGREGATION_MATCHER_RE = re.compile(
    r"AggregatingTransform: Aggregated. (?P<before_row_count>\d+) to (?P<after_row_count>\d+) rows \(from (?P<memory_size>.*)\) in (?P<seconds>[0-9.]+) sec. \((?P<rows_per_second>[0-9.]+) rows/sec., (?P<bytes_per_second>.+)/sec.\)"
)


@dataclass
class AggregationSummary:
    transform: str
    before_row_count: int
    after_row_count: int
    memory_size: str
    seconds: float
    rows_per_second: float
    bytes_per_second: str

    @staticmethod
    def from_log(log_line: str) -> AggregationSummary | None:
        match = AGGREGATION_MATCHER_RE.match(log_line)
        if not match:
            return None

        return AggregationSummary(
            transform="AggregatingTransform",
            before_row_count=int(match.group("before_row_count")),
            after_row_count=int(match.group("after_row_count")),
            memory_size=match.group("memory_size"),
            seconds=float(match.group("seconds")),
            rows_per_second=float(match.group("rows_per_second")),
            bytes_per_second=match.group("bytes_per_second"),
        )


# [ spans-clickhouse-1 ] [ 23107 ] {0.21246445055947638} <Debug> MergingSortedTransform: Merge sorted 2 blocks, 1458 rows in 0.464030493 sec., 3142.034892090594 rows/sec., 61.37 KiB/sec
SORTING_MATCHER_RE = re.compile(
    r"(?P<transform>Merging.*Transform): Merge sorted (?P<sorted_blocks>\d+) blocks, (?P<rows>\d+) rows in (?P<seconds>[0-9.]+) sec., (?P<rows_per_second>[0-9.]+) rows/sec., (?P<bytes_per_second>.+)/sec"
)


@dataclass
class SortingSummary:
    transform: str
    sorted_blocks: int
    rows: int
    seconds: float
    rows_per_second: float
    bytes_per_second: str

    @staticmethod
    def from_log(log_line: str) -> SortingSummary | None:
        match = SORTING_MATCHER_RE.match(log_line)
        if not match:
            return None

        return SortingSummary(
            transform=match.group("transform"),
            sorted_blocks=int(match.group("sorted_blocks")),
            rows=int(match.group("rows")),
            seconds=float(match.group("seconds")),
            rows_per_second=float(match.group("rows_per_second")),
            bytes_per_second=match.group("bytes_per_second"),
        )


# [ spans-clickhouse-1 ] [ 65011 ] {0.21246445055947638} <Debug> executeQuery: Read 8528171 rows, 886.74 MiB in 0.472519 sec., 18048313.40115424 rows/sec., 1.83 GiB/sec.
EXECUTE_MATCHER_RE = re.compile(
    r"executeQuery: Read (?P<rows_read>\d+) rows, (?P<memory_size>.+) in (?P<seconds>[0-9.]+) sec., (?P<rows_per_second>[0-9.]+) rows/sec., (?P<bytes_per_second>.+)/sec."
)


@dataclass
class ExecuteSummary:
    rows_read: int
    memory_size: str
    seconds: float
    rows_per_second: float
    bytes_per_second: str

    @staticmethod
    def from_log(log_line: str) -> ExecuteSummary | None:
        match = EXECUTE_MATCHER_RE.match(log_line)
        if not match:
            return None

        return ExecuteSummary(
            rows_read=int(match.group("rows_read")),
            memory_size=match.group("memory_size"),
            seconds=float(match.group("seconds")),
            rows_per_second=float(match.group("rows_per_second")),
            bytes_per_second=match.group("bytes_per_second"),
        )


@dataclass
class QuerySummary:
    node_name: str
    is_distributed: bool
    query_id: str
    execute_summaries: list[ExecuteSummary] | None = None
    select_summaries: list[SelectSummary] | None = None
    index_summaries: list[IndexSummary] | None = None
    stream_summaries: list[StreamSummary] | None = None
    aggregation_summaries: list[AggregationSummary] | None = None
    sorting_summaries: list[SortingSummary] | None = None


@dataclass
class TracingSummary:
    query_summaries: dict[str, QuerySummary]


line_types = [
    IndexSummary,
    SelectSummary,
    StreamSummary,
    AggregationSummary,
    SortingSummary,
    ExecuteSummary,
]


def summarize_trace_output(raw_trace_logs: str) -> TracingSummary:
    parsed = format_log_to_dict(raw_trace_logs)

    summary = TracingSummary({})
    query_node = parsed[0]["node_name"]
    summary.query_summaries[query_node] = QuerySummary(
        query_node, True, parsed[0]["query_id"]
    )
    for line in parsed:
        if line["node_name"] not in summary.query_summaries:
            summary.query_summaries[line["node_name"]] = QuerySummary(
                line["node_name"], False, line["query_id"]
            )

        query_summary = summary.query_summaries[line["node_name"]]
        for line_type in line_types:
            parsed_line = line_type.from_log(line["log_content"])  # type: ignore
            if parsed_line is not None:
                attr_name = (
                    line_type.__name__.lower().replace("summary", "") + "_summaries"
                )
                if getattr(query_summary, attr_name) is None:
                    setattr(query_summary, attr_name, [parsed_line])
                else:
                    getattr(query_summary, attr_name).append(parsed_line)

    return summary


square_re = re.compile(r"\[.*?\]")
angle_re = re.compile(r"<.*?>")
curly_re = re.compile(r"{.*?}")


def format_log_to_dict(raw_trace_logs: str) -> list[dict[str, Any]]:
    # CLICKHOUSE TRACING LOG STRUCTURE: '[ NODE ] [ THREAD_ID ] {QUERY_ID} <LOG_TYPE> LOG_LINE'
    formatted_logs = []
    for line in raw_trace_logs.splitlines():
        context = square_re.findall(line)
        log_type = ""
        log_type_regex = angle_re.search(line)
        if log_type_regex is not None:
            log_type = log_type_regex.group()

        query_id = ""
        query_id_regex = curly_re.search(line)
        if query_id_regex is not None:
            query_id = query_id_regex.group()

        try:
            formatted_log = {
                "node_name": context[0][2:-2],
                "thread_id": context[1][2:-2],
                "query_id": query_id[1:-1],
                "log_type": log_type,
                "log_content": angle_re.split(line)[1].strip(),
            }
            formatted_logs.append(formatted_log)
        except Exception:
            # Error parsing log line, continue.
            pass

    return formatted_logs
