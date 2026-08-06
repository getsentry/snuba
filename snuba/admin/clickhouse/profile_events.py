import json
import time
from typing import cast

import structlog

from snuba.admin.clickhouse.common import get_ro_query_node_connection
from snuba.admin.clickhouse.tracing import TraceOutput, system_log_source
from snuba.clickhouse.native import ClickhousePool, ClickhouseResult
from snuba.clusters.cluster import ClickhouseClientSettings
from snuba.utils.constants import (
    PROFILE_EVENTS_MAX_ATTEMPTS,
    PROFILE_EVENTS_MAX_WAIT_SECONDS,
)

logger = structlog.get_logger().bind(module=__name__)


def gather_profile_events(query_trace: TraceOutput, storage: str) -> None:
    """
    Collect ProfileEvents from system.query_log for the traced query.

    Uses the storage query-node connection and clusterAllReplicas when
    available, so this path stays driver-agnostic: no per-host native/HTTP
    ports are chosen here.
    """
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
        WHERE event_date >= yesterday()
          AND type = 'QueryFinish'
          AND query_id IN ({id_list})
    """

    result = _poll_profile_events(connection, sql)
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


def _poll_profile_events(connection: ClickhousePool, sql: str) -> ClickhouseResult | None:
    wait_time = 1
    last_result: ClickhouseResult | None = None
    for attempt in range(PROFILE_EVENTS_MAX_ATTEMPTS):
        try:
            last_result = connection.execute(query=sql, with_column_types=True)
        except Exception:
            logger.warning("Profile events poll failed", attempt=attempt, exc_info=True)
            last_result = None

        if last_result is not None and last_result.results:
            return last_result

        if attempt + 1 < PROFILE_EVENTS_MAX_ATTEMPTS:
            wait_time = min(wait_time * 2, PROFILE_EVENTS_MAX_WAIT_SECONDS)
            time.sleep(wait_time)

    return last_result
