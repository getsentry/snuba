from __future__ import annotations

import time
from collections.abc import Sequence
from concurrent.futures import Future, ThreadPoolExecutor
from concurrent.futures import TimeoutError as FutureTimeoutError
from typing import NamedTuple, TypedDict

import structlog

from snuba.admin.clickhouse.common import get_ro_cluster_node_connection
from snuba.clickhouse.escaping import escape_string
from snuba.clusters.cluster import CLUSTERS, ClickhouseClientSettings, ClickhouseCluster

logger = structlog.get_logger().bind(module=__name__)

# One lookup per configured Snuba cluster, run concurrently so that a single
# slow or unreachable cluster does not hold up the whole page.
MAX_CONCURRENT_CLUSTER_QUERIES = 32
CLUSTER_QUERY_TIMEOUT = 30

# Tables are only listed for this database, the one Snuba's own tables live in.
# Interpolated into the query below, so it must stay a literal we control.
TABLES_DATABASE = "default"


class NodeVersionInfo(TypedDict):
    # Kept for compatibility with frontend bundles from before cluster-level
    # version lookups. These identify a ClickHouse cluster, not an individual
    # host, and contain one entry per distinct version.
    host: str
    port: int
    version: str | None
    error: str | None


class ClusterInfo(TypedDict):
    # The cluster's configured query endpoint (an actual node or a proxy).
    host: str
    port: int
    database: str
    secure: bool
    single_node: bool
    # The ClickHouse cluster names, as they appear in system.clusters. These are
    # only set for multi node clusters.
    cluster_name: str | None
    distributed_cluster_name: str | None
    storage_sets: Sequence[str]
    query_cluster_versions: Sequence[str]
    query_node_versions: Sequence[NodeVersionInfo]
    query_node_error: str | None
    storage_cluster_versions: Sequence[str]
    storage_node_versions: Sequence[NodeVersionInfo]
    storage_node_error: str | None
    # `tables` contains the tables in the `default` database on the configured
    # query endpoint. It is empty if the lookup failed, in which case `error`
    # explains why.
    tables: Sequence[str]
    error: str | None


def _describe_cluster(cluster: ClickhouseCluster) -> ClusterInfo:
    """Return the statically configured properties of a cluster."""
    return {
        "host": cluster.get_host(),
        "port": cluster.get_port(),
        "database": cluster.get_database(),
        "secure": cluster.get_secure(),
        "single_node": cluster.is_single_node(),
        "cluster_name": cluster.get_clickhouse_cluster_name(),
        "distributed_cluster_name": cluster.get_clickhouse_distributed_cluster_name(),
        "storage_sets": sorted(storage_set.value for storage_set in cluster.get_storage_set_keys()),
        "query_cluster_versions": [],
        "query_node_versions": [],
        "query_node_error": None,
        "storage_cluster_versions": [],
        "storage_node_versions": [],
        "storage_node_error": None,
        "tables": [],
        "error": None,
    }


class _ClusterState(NamedTuple):
    query_cluster_versions: Sequence[str]
    query_node_error: str | None
    storage_cluster_versions: Sequence[str]
    storage_node_error: str | None
    tables: Sequence[str]
    error: str | None


def _get_versions(cluster: ClickhouseCluster, cluster_name: str | None) -> Sequence[str]:
    query_node = cluster.get_query_node()
    source = (
        f"clusterAllReplicas({escape_string(cluster_name)}, system.one)"
        if cluster_name is not None
        else "system.one"
    )
    results = (
        get_ro_cluster_node_connection(
            cluster,
            query_node,
            ClickhouseClientSettings.QUERY,
            known_nodes=[query_node],
        )
        .execute(f"SELECT DISTINCT version() AS version FROM {source} ORDER BY version")
        .results
    )
    return [str(row[0]) for row in results]


def _get_tables(cluster: ClickhouseCluster) -> Sequence[str]:
    query_node = cluster.get_query_node()
    results = (
        get_ro_cluster_node_connection(
            cluster,
            query_node,
            ClickhouseClientSettings.QUERY,
            known_nodes=[query_node],
        )
        .execute(
            f"""
            SELECT arraySort(groupUniqArray(name))
            FROM system.tables
            WHERE database = '{TABLES_DATABASE}'
            """
        )
        .results
    )
    if not results:
        return []
    return [str(table) for table in results[0][0]]


def _future_result(
    future: Future[Sequence[str]],
    deadline: float,
    cluster: ClickhouseCluster,
    lookup: str,
) -> tuple[Sequence[str], str | None]:
    try:
        result = future.result(timeout=max(0, deadline - time.monotonic()))
        return result, None
    except Exception as e:
        error = (
            f"Timed out after {CLUSTER_QUERY_TIMEOUT}s"
            if isinstance(e, FutureTimeoutError)
            else str(e)
        )
        logger.warning(error, cluster=str(cluster), lookup=lookup)
        return [], error


def _get_cluster_state(cluster: ClickhouseCluster, deadline: float | None = None) -> _ClusterState:
    """Fetch distinct versions for each ClickHouse cluster and local tables."""
    if deadline is None:
        deadline = time.monotonic() + CLUSTER_QUERY_TIMEOUT

    query_cluster_name = cluster.get_clickhouse_distributed_cluster_name()
    storage_cluster_name = cluster.get_clickhouse_cluster_name()

    # A single-node deployment has no ClickHouse cluster names. Run one local
    # distinct-version query and use its result for both roles.
    if cluster.is_single_node():
        executor = ThreadPoolExecutor(max_workers=2)
        try:
            versions_future = executor.submit(_get_versions, cluster, None)
            tables_future = executor.submit(_get_tables, cluster)
            versions, version_error = _future_result(versions_future, deadline, cluster, "versions")
            tables, table_error = _future_result(tables_future, deadline, cluster, "tables")
        finally:
            executor.shutdown(wait=False, cancel_futures=True)
        return _ClusterState(
            versions,
            version_error,
            versions,
            version_error,
            tables,
            table_error,
        )

    executor = ThreadPoolExecutor(max_workers=3)
    try:
        query_versions_future = executor.submit(_get_versions, cluster, query_cluster_name)
        storage_versions_future = executor.submit(_get_versions, cluster, storage_cluster_name)
        tables_future = executor.submit(_get_tables, cluster)
        query_versions, query_error = _future_result(
            query_versions_future, deadline, cluster, "query_cluster_versions"
        )
        storage_versions, storage_error = _future_result(
            storage_versions_future, deadline, cluster, "storage_cluster_versions"
        )
        tables, table_error = _future_result(tables_future, deadline, cluster, "tables")
    finally:
        executor.shutdown(wait=False, cancel_futures=True)

    return _ClusterState(
        query_versions,
        query_error,
        storage_versions,
        storage_error,
        tables,
        table_error,
    )


def _compat_versions(
    cluster_name: str | None, versions: Sequence[str]
) -> Sequence[NodeVersionInfo]:
    label = cluster_name or "single node"
    return [{"host": label, "port": 0, "version": version, "error": None} for version in versions]


def get_cluster_info() -> Sequence[ClusterInfo]:
    """Describe every configured cluster and its distinct ClickHouse versions."""
    if not CLUSTERS:
        return []

    cluster_info = [_describe_cluster(cluster) for cluster in CLUSTERS]

    executor = ThreadPoolExecutor(max_workers=min(MAX_CONCURRENT_CLUSTER_QUERIES, len(CLUSTERS)))
    try:
        deadline = time.monotonic() + CLUSTER_QUERY_TIMEOUT
        states = [executor.submit(_get_cluster_state, cluster, deadline) for cluster in CLUSTERS]
        for cluster, info, state in zip(CLUSTERS, cluster_info, states, strict=True):
            try:
                (
                    info["query_cluster_versions"],
                    info["query_node_error"],
                    info["storage_cluster_versions"],
                    info["storage_node_error"],
                    info["tables"],
                    info["error"],
                ) = state.result(timeout=max(0, deadline - time.monotonic()))
                info["query_node_versions"] = _compat_versions(
                    info["distributed_cluster_name"], info["query_cluster_versions"]
                )
                info["storage_node_versions"] = _compat_versions(
                    info["cluster_name"], info["storage_cluster_versions"]
                )
            except FutureTimeoutError:
                error = f"Timed out after {CLUSTER_QUERY_TIMEOUT}s"
                info["query_node_error"] = error
                info["storage_node_error"] = error
                info["error"] = error
            except Exception as e:
                logger.warning(str(e), cluster=str(cluster))
                error = str(e)
                info["query_node_error"] = error
                info["storage_node_error"] = error
                info["error"] = error
    finally:
        executor.shutdown(wait=False, cancel_futures=True)

    return cluster_info
