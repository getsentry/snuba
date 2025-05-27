import json
import socket
import time
from typing import Dict, List, cast

import structlog
from flask import g

from snuba.admin.clickhouse.common import InvalidNodeError
from snuba.admin.clickhouse.system_queries import run_system_query_on_host_with_sql
from snuba.admin.clickhouse.tracing import QueryTraceData, TraceOutput
from snuba.utils.constants import (
    PROFILE_EVENTS_MAX_ATTEMPTS,
    PROFILE_EVENTS_MAX_WAIT_SECONDS,
)

logger = structlog.get_logger().bind(module=__name__)


def gather_profile_events(query_trace: TraceOutput, storage: str) -> None:
    """
    Gathers profile events for each query trace and updates the query_trace object with results.
    Uses exponential backoff when polling for results.

    Args:
        query_trace: TraceOutput object to update with profile events
        storage: Storage identifier
    """
    profile_events_raw_sql = "SELECT ProfileEvents FROM system.query_log WHERE query_id = '{}' AND type = 'QueryFinish'"

    for query_trace_data in parse_trace_for_query_ids(query_trace):
        sql = profile_events_raw_sql.format(query_trace_data.query_id)
        logger.info(
            "Gathering profile event using host: {}, port = {}, storage = {}, sql = {}, g.user = {}".format(
                query_trace_data.host, query_trace_data.port, storage, sql, g.user
            )
        )

        system_query_result = None
        attempt = 0
        wait_time = 1
        while attempt < PROFILE_EVENTS_MAX_ATTEMPTS:
            try:
                system_query_result = run_system_query_on_host_with_sql(
                    query_trace_data.host,
                    int(query_trace_data.port),
                    storage,
                    sql,
                    False,
                    g.user,
                )
            except InvalidNodeError as exc:
                # this can happen for the abnormal storage sets like
                # discover and errors_ro, where the query_trace_data host
                # and port don't match the cluster definitions
                logger.error(exc, exc_info=True)
                break

            if system_query_result.results:
                break

            wait_time = min(wait_time * 2, PROFILE_EVENTS_MAX_WAIT_SECONDS)
            time.sleep(wait_time)
            attempt += 1

        if system_query_result is not None and len(system_query_result.results) > 0:
            query_trace.profile_events_meta.append(system_query_result.meta)
            query_trace.profile_events_profile = cast(
                Dict[str, int], system_query_result.profile
            )
            columns = system_query_result.meta
            if columns:
                res = {}
                res["column_names"] = [name for name, _ in columns]
                res["rows"] = []
                for query_result in system_query_result.results:
                    if query_result[0]:
                        res["rows"].append(json.dumps(query_result[0]))
                query_trace.profile_events_results[query_trace_data.node_name] = res


def hostname_resolves(hostname: str) -> bool:
    try:
        socket.gethostbyname(hostname)
    except socket.error:
        return False
    else:
        return True


def parse_trace_for_query_ids(trace_output: TraceOutput) -> List[QueryTraceData]:
    summarized_trace_output = trace_output.summarized_trace_output
    node_name_to_query_id = {
        node_name: query_summary.query_id
        for node_name, query_summary in summarized_trace_output.query_summaries.items()
    }
    logger.info("node to query id mapping: {}".format(node_name_to_query_id))
    return [
        QueryTraceData(
            host=node_name if hostname_resolves(node_name) else "127.0.0.1",
            port=9000,
            query_id=query_id,
            node_name=node_name,
        )
        for node_name, query_id in node_name_to_query_id.items()
    ]
