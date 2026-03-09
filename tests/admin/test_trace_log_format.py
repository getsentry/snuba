from snuba.admin.clickhouse.trace_log_parsing import (
    AggregationSummary,
    ExecuteSummary,
    IndexSummary,
    SelectSummary,
    SortingSummary,
    StreamSummary,
    summarize_trace_output,
)
from tests.fixtures import get_raw_join_query_trace


def test_tracing_summary() -> None:
    output = summarize_trace_output(get_raw_join_query_trace())
    assert set(output.query_summaries.keys()) == {
        "query-node-1",
        "storage-node-1",
        "storage-node-2",
    }
    dist_summary = output.query_summaries["query-node-1"]
    assert dist_summary.query_id == "06bba25b-06e9-4014-9207-ddaf79d89cbc"
    assert dist_summary.is_distributed is True
    assert dist_summary.execute_summaries is not None and dist_summary.execute_summaries[
        0
    ] == ExecuteSummary(
        rows_read=20826,
        memory_size="41.62 MiB",
        seconds=0.300817,
        rows_per_second=7715075.943181403,
        bytes_per_second="138.36 MiB",
    )
    assert dist_summary.sorting_summaries is not None and dist_summary.sorting_summaries[
        0
    ] == SortingSummary(
        transform="MergingSortedTransform",
        sorted_blocks=2,
        rows=58,
        seconds=0.464030493,
        rows_per_second=3142.034892090594,
        bytes_per_second="61.37 KiB",
    )

    oneone_summary = output.query_summaries["storage-node-1"]
    assert oneone_summary.query_id == "7d2d32ee-cb6b-4cdb-87cc-e7c8ba6fb8ec"
    assert oneone_summary.is_distributed is False
    assert oneone_summary.execute_summaries is not None and oneone_summary.execute_summaries[
        0
    ] == ExecuteSummary(
        rows_read=17264,
        memory_size="41.55 MiB",
        seconds=0.082856,
        rows_per_second=27967365.067104373,
        bytes_per_second="501.49 MiB",
    )
    assert oneone_summary.select_summaries is not None and oneone_summary.select_summaries[
        0
    ] == SelectSummary(
        table_name="default.errors_local",
        parts_selected_by_partition_key=1,
        total_parts=7,
        parts_selected_by_primary_key=1,
        marks_selected_by_primary_key=78,
        total_marks=89,
        marks_to_read_from_ranges=78,
    )
    assert (
        oneone_summary.aggregation_summaries is not None
        and oneone_summary.aggregation_summaries[0]
        == AggregationSummary(
            transform="AggregatingTransform",
            before_row_count=0,
            after_row_count=0,
            memory_size="0.00 B",
            seconds=0.03458627,
            rows_per_second=0.0,
            bytes_per_second="0.00 B",
        )
    )

    onetwo_summary = output.query_summaries["storage-node-2"]
    assert onetwo_summary.query_id == "f3fb112a-583f-4125-a424-bd1d21b6ecf2"
    assert onetwo_summary.is_distributed is False
    assert onetwo_summary.execute_summaries is not None and onetwo_summary.execute_summaries[
        0
    ] == ExecuteSummary(
        rows_read=562,
        memory_size="73.06 KiB",
        seconds=0.193038,
        rows_per_second=18452.325448875352,
        bytes_per_second="378.50 KiB",
    )
    assert onetwo_summary.select_summaries is not None and onetwo_summary.select_summaries[
        0
    ] == SelectSummary(
        table_name="default.transactions_local",
        parts_selected_by_partition_key=1,
        total_parts=5,
        parts_selected_by_primary_key=1,
        marks_selected_by_primary_key=19,
        total_marks=68,
        marks_to_read_from_ranges=1,
    )
    assert onetwo_summary.index_summaries is not None and onetwo_summary.index_summaries[
        0
    ] == IndexSummary(
        table_name="default.transactions_local",
        index_name="bf_trace_id",
        dropped_granules=18,
        total_granules=19,
    )
    assert (
        onetwo_summary.aggregation_summaries is not None
        and onetwo_summary.aggregation_summaries[0]
        == AggregationSummary(
            transform="AggregatingTransform",
            before_row_count=1,
            after_row_count=1,
            memory_size="17.00 B",
            seconds=0.024679052,
            rows_per_second=40.52,
            bytes_per_second="688.84 B",
        )
    )
    assert onetwo_summary.stream_summaries is not None and onetwo_summary.stream_summaries[
        0
    ] == StreamSummary(table_name="default.transactions_local", approximate_rows=86299, streams=4)
