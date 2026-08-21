from __future__ import annotations

import time
from collections.abc import Sequence
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FutureTimeoutError
from typing import NamedTuple, TypedDict

import structlog

from snuba.admin.clickhouse.common import get_ro_cluster_node_connection
from snuba.clickhouse.escaping import escape_string
from snuba.clusters.cluster import CLUSTERS, ClickhouseClientSettings, ClickhouseCluster

logger = structlog.get_logger().bind(module=__name__)

MAX_CONCURRENT_CLUSTER_QUERIES = 32
CLUSTER_QUERY_TIMEOUT = 30
TABLES_DATABASE = "default"


class ClusterInfo(TypedDict):
    cluster_name: str
    versions: Sequence[str]
    storage_sets: Sequence[str]
    tables: Sequence[str]
    error: str | None


class _ClusterTarget(NamedTuple):
    # Unique identity used while collapsing duplicate ClickHouse names.
    key: str
    # Value shown in the Cluster Name column.
    name: str
    cluster: ClickhouseCluster
    storage_sets: set[str]
    local: bool


class _ClusterState(NamedTuple):
    versions: Sequence[str]
    tables: Sequence[str]


def _cluster_targets() -> Sequence[_ClusterTarget]:
    """Return one target per unique ClickHouse cluster endpoint/name."""
    targets: dict[str, _ClusterTarget] = {}
    for cluster in CLUSTERS:
        storage_sets = {storage_set.value for storage_set in cluster.get_storage_set_keys()}
        if cluster.is_single_node():
            host = cluster.get_host()
            port = cluster.get_port()
            # Single-node configs have no ClickHouse cluster name. Keep each
            # endpoint separate so multiple single-node hosts do not collapse.
            entries = [(f"{host}:{port}", "single node", True)]
        else:
            entries = [
                (name, name, False)
                for name in (
                    cluster.get_clickhouse_distributed_cluster_name(),
                    cluster.get_clickhouse_cluster_name(),
                )
                if name is not None
            ]

        for key, name, local in entries:
            existing = targets.get(key)
            if existing is not None:
                existing.storage_sets.update(storage_sets)
                continue
            # Copy storage_sets so query/storage siblings from one Snuba cluster
            # do not share one mutable set and later merges stay isolated.
            targets[key] = _ClusterTarget(key, name, cluster, set(storage_sets), local)
    return list(targets.values())


def _cluster_source(target: _ClusterTarget, table: str) -> str:
    if target.local:
        return table
    return f"clusterAllReplicas({escape_string(target.name)}, {table})"


def _get_cluster_state(target: _ClusterTarget) -> _ClusterState:
    """Fetch distinct versions and default-database tables for one cluster."""
    query_node = target.cluster.get_query_node()
    connection = get_ro_cluster_node_connection(
        target.cluster,
        query_node,
        ClickhouseClientSettings.QUERY,
        known_nodes=[query_node],
    )
    # system.one always yields one row per replica, including replicas with no
    # tables in `default`. Tables are collected separately so empty default DBs
    # do not drop version rows.
    version_rows = connection.execute(
        f"""
        SELECT DISTINCT version() AS version
        FROM {_cluster_source(target, "system.one")}
        ORDER BY version
        """
    ).results
    table_rows = connection.execute(
        f"""
        SELECT arraySort(groupUniqArray(name)) AS tables
        FROM {_cluster_source(target, "system.tables")}
        WHERE database = '{TABLES_DATABASE}'
        """
    ).results
    versions = [str(row[0]) for row in version_rows]
    tables = sorted({str(table) for row in table_rows for table in row[0]})
    return _ClusterState(versions, tables)


def _describe_target(
    target: _ClusterTarget,
    versions: Sequence[str] = (),
    tables: Sequence[str] = (),
    error: str | None = None,
) -> ClusterInfo:
    return {
        "cluster_name": target.name,
        "versions": versions,
        "storage_sets": sorted(target.storage_sets),
        "tables": tables,
        "error": error,
    }


def get_cluster_info() -> Sequence[ClusterInfo]:
    """Describe each ClickHouse cluster used by this Snuba deployment."""
    targets = _cluster_targets()
    if not targets:
        return []

    deadline = time.monotonic() + CLUSTER_QUERY_TIMEOUT
    info: list[ClusterInfo] = []
    executor = ThreadPoolExecutor(max_workers=min(MAX_CONCURRENT_CLUSTER_QUERIES, len(targets)))
    try:
        states = [executor.submit(_get_cluster_state, target) for target in targets]
        for target, state in zip(targets, states, strict=True):
            try:
                versions, tables = state.result(timeout=max(0, deadline - time.monotonic()))
                info.append(_describe_target(target, versions, tables))
            except FutureTimeoutError:
                error = f"Timed out after {CLUSTER_QUERY_TIMEOUT}s"
                info.append(_describe_target(target, error=error))
            except Exception as e:
                logger.warning(str(e), cluster=target.name)
                info.append(_describe_target(target, error=str(e)))
    finally:
        executor.shutdown(wait=False, cancel_futures=True)

    return info
