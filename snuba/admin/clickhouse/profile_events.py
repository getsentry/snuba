import json
from typing import cast

import structlog

from snuba.admin.clickhouse.common import get_ro_query_node_connection
from snuba.admin.clickhouse.tracing import TraceOutput, poll_system_query, system_log_source
from snuba.clusters.cluster import ClickhouseClientSettings

logger = structlog.get_logger().bind(module=__name__)


def gather_profile_events(query_trace: TraceOutput, storage: str) -> None:
    query_ids = _profile_event_query_ids(query_trace)
    if not query_ids:
        return

    connection = get_ro_query_node_connection(storage, ClickhouseClientSettings.QUERY)
    source = system_log_source(storage, "query_log")
    id_list = ", ".join(f"'{query_id}'" for query_id in query_ids)
    sql = f"""
        SELECT
            hostname() AS host,
            ProfileEvents
        FROM {source}
        WHERE event_time >= now() - INTERVAL 5 MINUTE
          AND type = 'QueryFinish'
          AND query_id IN ({id_list})
    """

    result = poll_system_query(connection, sql)
    if result is None or not result.results:
        return

    query_trace.profile_events_meta.append(result.meta)
    if result.profile is not None:
        query_trace.profile_events_profile = cast(dict[str, int], result.profile)

    columns = result.meta or []
    column_names = [name for name, _ in columns]
    for row in result.results:
        if len(row) < 2 or not row[1]:
            continue
        host = str(row[0])
        query_trace.profile_events_results[host] = {
            "column_names": column_names,
            "rows": [json.dumps(row[1])],
        }


def _profile_event_query_ids(query_trace: TraceOutput) -> list[str]:
    ids = {
        query_summary.query_id
        for query_summary in query_trace.summarized_trace_output.query_summaries.values()
        if query_summary.query_id
    }
    if query_trace.query_id:
        ids.add(query_trace.query_id)
    return sorted(ids)
