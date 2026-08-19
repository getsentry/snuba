import json
from typing import cast

import structlog

from snuba.admin.clickhouse.common import get_ro_query_node_connection
from snuba.admin.clickhouse.tracing import TraceOutput, poll_system_query, system_log_source
from snuba.clickhouse.escaping import escape_string
from snuba.clickhouse.pool import ClickhouseResult
from snuba.clusters.cluster import ClickhouseClientSettings

logger = structlog.get_logger().bind(module=__name__)


def gather_profile_events(query_trace: TraceOutput, storage: str) -> None:
    query_ids = _profile_event_query_ids(query_trace)
    if not query_ids:
        return

    connection = get_ro_query_node_connection(storage, ClickhouseClientSettings.QUERY)
    source = system_log_source(storage, "query_log")
    id_list = ", ".join(escape_string(query_id) for query_id in query_ids)
    sql = f"""
        SELECT
            query_id,
            hostname() AS host,
            ProfileEvents
        FROM {source}
        WHERE event_time >= now() - INTERVAL 5 MINUTE
          AND type = 'QueryFinish'
          AND query_id IN ({id_list})
    """

    expected_query_ids = set(query_ids)

    def _all_query_ids_present(result: ClickhouseResult) -> bool:
        if not result.results:
            return False
        seen = {str(row[0]) for row in result.results if row}
        return expected_query_ids.issubset(seen)

    # Wait until every requested query_id has flushed, not just the first node.
    result = poll_system_query(connection, sql, accept_result=_all_query_ids_present)
    if result is None or not result.results:
        return

    # Keep the historical single-column ProfileEvents meta the frontend expects.
    query_trace.profile_events_meta.append(
        [col for col in (result.meta or []) if col[0] == "ProfileEvents"] or result.meta
    )
    if result.profile is not None:
        query_trace.profile_events_profile = cast(dict[str, int], result.profile)

    # Prefer summary node_name keys (same as the old per-host path) when we can
    # map by query_id; otherwise fall back to hostname().
    query_id_to_node = {
        summary.query_id: node_name
        for node_name, summary in query_trace.summarized_trace_output.query_summaries.items()
        if summary.query_id
    }

    for row in result.results:
        if len(row) < 3 or not row[2]:
            continue
        row_query_id = str(row[0])
        hostname = str(row[1])
        profile_events = row[2]
        node_name = query_id_to_node.get(row_query_id, hostname)
        host_result = query_trace.profile_events_results.setdefault(
            node_name,
            {
                "column_names": ["ProfileEvents"],
                "rows": [],
            },
        )
        cast(list[str], host_result["rows"]).append(json.dumps(profile_events))


def _profile_event_query_ids(query_trace: TraceOutput) -> list[str]:
    ids = {
        query_summary.query_id
        for query_summary in query_trace.summarized_trace_output.query_summaries.values()
        if query_summary.query_id
    }
    if query_trace.query_id:
        ids.add(query_trace.query_id)
    return sorted(ids)
