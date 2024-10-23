from typing import List

from sentry_protos.snuba.v1.request_common_pb2 import (
    QueryInfo,
    QueryMetadata,
    QueryStats,
    ResponseMeta,
    TimingMarks,
)

from snuba.utils.metrics.timer import Timer
from snuba.web import QueryResult


def extract_response_meta(
    request_id: str, debug: bool, query_results: List[QueryResult], timers: List[Timer]
) -> ResponseMeta:
    query_info = []

    if debug:
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

            query_info.append(QueryInfo(stats=query_stats, metadata=query_metadata))

    return ResponseMeta(request_id=request_id, query_info=query_info)
