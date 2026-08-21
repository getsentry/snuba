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
SINGLE_NODE_CLUSTER_NAME = "single node"


class NodeVersionInfo(TypedDict):
    """Compatibility shape for frontend bundles from before cluster-level rows."""

    host: str
    port: int
    version: str | None
    error: str | None


class ClusterInfo(TypedDict):
    cluster_name: str
    versions: Sequence[str]
    storage_sets: Sequence[str]
    tables: Sequence[str]
    error: str | None
    # Compatibility fields for frontend bundles from before cluster-level rows.
    host: str
    port: int
    database: str
    secure: bool
    single_node: bool
    distributed_cluster_name: str | None
    query_cluster_versions: Sequence[str]
    query_node_versions: Sequence[NodeVersionInfo]
    query_node_error: str | None
    storage_cluster_versions: Sequence[str]
    storage_node_versions: Sequence[NodeVersionInfo]
    storage_node_error: str | None


class _ClusterTarget(NamedTuple):
    name: str
    cluster: ClickhouseCluster
    storage_sets: set[str]
    local: bool


class _ClusterState(NamedTuple):
    versions: Sequence[str]
    tables: Sequence[str]


def _cluster_targets() -> Sequence[_ClusterTarget]:
    """Return one target per unique ClickHouse cluster name."""
    targets: dict[str, _ClusterTarget] = {}
    for cluster in CLUSTERS:
        storage_sets = {storage_set.value for storage_set in cluster.get_storage_set_keys()}
        names = (
            [(SINGLE_NODE_CLUSTER_NAME, True)]
            if cluster.is_single_node()
            else [
                (cluster.get_clickhouse_distributed_cluster_name(), False),
                (cluster.get_clickhouse_cluster_name(), False),
            ]
        )
        for name, local in names:
            if name is None:
                continue
            if name in targets:
                targets[name].storage_sets.update(storage_sets)
            else:
                targets[name] = _ClusterTarget(name, cluster, storage_sets, local)
    return list(targets.values())


def _get_cluster_state(target: _ClusterTarget) -> _ClusterState:
    """Fetch distinct versions and default-database tables for one cluster."""
    query_node = target.cluster.get_query_node()
    source = (
        "system.tables"
        if target.local
        else f"clusterAllReplicas({escape_string(target.name)}, system.tables)"
    )
    results = (
        get_ro_cluster_node_connection(
            target.cluster,
            query_node,
            ClickhouseClientSettings.QUERY,
            known_nodes=[query_node],
        )
        .execute(
            f"""
            SELECT
                version() AS version,
                arraySort(groupUniqArray(name)) AS tables
            FROM {source}
            WHERE database = '{TABLES_DATABASE}'
            GROUP BY version
            ORDER BY version
            """
        )
        .results
    )
    versions = sorted({str(row[0]) for row in results})
    tables = sorted({str(table) for row in results for table in row[1]})
    return _ClusterState(versions, tables)


def _compat_versions(name: str, versions: Sequence[str]) -> Sequence[NodeVersionInfo]:
    return [{"host": name, "port": 0, "version": version, "error": None} for version in versions]


def _describe_target(
    target: _ClusterTarget,
    versions: Sequence[str] = (),
    tables: Sequence[str] = (),
    error: str | None = None,
) -> ClusterInfo:
    compat_versions = _compat_versions(target.name, versions)
    return {
        "cluster_name": target.name,
        "versions": versions,
        "storage_sets": sorted(target.storage_sets),
        "tables": tables,
        "error": error,
        "host": target.cluster.get_host(),
        "port": target.cluster.get_port(),
        "database": TABLES_DATABASE,
        "secure": target.cluster.get_secure(),
        "single_node": target.local,
        "distributed_cluster_name": target.name,
        "query_cluster_versions": versions,
        "query_node_versions": compat_versions,
        "query_node_error": error,
        "storage_cluster_versions": versions,
        "storage_node_versions": compat_versions,
        "storage_node_error": error,
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
