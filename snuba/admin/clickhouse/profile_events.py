import json
import time
from typing import cast

import structlog
from flask import g

from snuba.admin.clickhouse.common import InvalidNodeError
from snuba.admin.clickhouse.system_queries import run_system_query_on_host_with_sql
from snuba.admin.clickhouse.tracing import QueryTraceData, TraceOutput
from snuba.datasets.storages.factory import get_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.utils.constants import (
    PROFILE_EVENTS_MAX_ATTEMPTS,
    PROFILE_EVENTS_MAX_WAIT_SECONDS,
)

logger = structlog.get_logger().bind(module=__name__)


def gather_profile_events(query_trace: TraceOutput, storage: str) -> None:
    profile_events_raw_sql = (
        "SELECT ProfileEvents FROM system.query_log WHERE query_id = '{}' AND type = 'QueryFinish'"
    )

    for query_trace_data in parse_trace_for_query_ids(query_trace, storage):
        sql = profile_events_raw_sql.format(query_trace_data.query_id)

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
                    False,
                    g.user,
                )
            except InvalidNodeError as exc:
                logger.error(exc, exc_info=True)
                break

            if system_query_result.results:
                break

            wait_time = min(wait_time * 2, PROFILE_EVENTS_MAX_WAIT_SECONDS)
            time.sleep(wait_time)
            attempt += 1

        if system_query_result is not None and len(system_query_result.results) > 0:
            query_trace.profile_events_meta.append(system_query_result.meta)
            query_trace.profile_events_profile = cast(dict[str, int], system_query_result.profile)
            columns = system_query_result.meta
            if columns:
                res: dict[str, object] = {}
                res["column_names"] = [name for name, _ in columns]
                rows: list[str] = []
                for query_result in system_query_result.results:
                    if query_result[0]:
                        rows.append(json.dumps(query_result[0]))
                res["rows"] = rows
                query_trace.profile_events_results[query_trace_data.node_name] = res


def _cluster_host_ports(storage: str) -> dict[str, int]:
    try:
        cluster = get_storage(StorageKey(storage)).get_cluster()
        nodes = [
            cluster.get_query_node(),
            *cluster.get_local_nodes(),
            *cluster.get_distributed_nodes(),
        ]
        return {node.host_name: node.native_port for node in nodes}
    except Exception:
        logger.warning(
            "Could not resolve cluster nodes for profile event hosts",
            storage=storage,
            exc_info=True,
        )
        return {}


def _query_node_trace_data(storage: str, query_id: str) -> QueryTraceData | None:
    try:
        query_node = get_storage(StorageKey(storage)).get_cluster().get_query_node()
        return QueryTraceData(
            host=query_node.host_name,
            port=query_node.native_port,
            query_id=query_id,
            node_name=query_node.host_name,
        )
    except Exception:
        logger.warning(
            "Could not resolve query node for profile events fallback",
            storage=storage,
            exc_info=True,
        )
        return None


def parse_trace_for_query_ids(
    trace_output: TraceOutput, storage: str | None = None
) -> list[QueryTraceData]:
    summarized_trace_output = trace_output.summarized_trace_output
    node_name_to_query_id = {
        node_name: query_summary.query_id
        for node_name, query_summary in summarized_trace_output.query_summaries.items()
    }

    if not node_name_to_query_id:
        if trace_output.query_id and storage:
            fallback = _query_node_trace_data(storage, trace_output.query_id)
            return [fallback] if fallback is not None else []
        return []

    if not storage:
        return []

    host_ports = _cluster_host_ports(storage)
    results: list[QueryTraceData] = []
    for node_name, query_id in node_name_to_query_id.items():
        # Only open connections to hosts we can validate against the cluster.
        # is_valid_node keys off native_port; inventing 9000/8123 for unknown
        # hostnames just fails validation.
        if node_name not in host_ports:
            logger.warning(
                "Skipping profile events for unknown host",
                host=node_name,
                storage=storage,
            )
            continue
        results.append(
            QueryTraceData(
                host=node_name,
                port=host_ports[node_name],
                query_id=query_id,
                node_name=node_name,
            )
        )

    if not results and trace_output.query_id:
        fallback = _query_node_trace_data(storage, trace_output.query_id)
        if fallback is not None:
            return [fallback]

    return results
