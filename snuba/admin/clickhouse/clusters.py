from __future__ import annotations

import time
from collections.abc import Sequence
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FutureTimeoutError
from typing import NamedTuple, TypedDict

import structlog

from snuba.admin.clickhouse.common import get_ro_cluster_node_connection
from snuba.clusters.cluster import (
    CLUSTERS,
    ClickhouseClientSettings,
    ClickhouseCluster,
    ClickhouseNode,
)

logger = structlog.get_logger().bind(module=__name__)

# One lookup per cluster, run concurrently so that a single slow or unreachable
# cluster does not hold up the whole page.
MAX_CONCURRENT_CLUSTER_QUERIES = 32
# Node version lookups within one cluster also run concurrently so one slow
# replica cannot burn the whole deadline before tables/other nodes are fetched.
MAX_CONCURRENT_NODE_QUERIES = 16
CLUSTER_QUERY_TIMEOUT = 30

# Tables are only listed for this database, the one Snuba's own tables live in.
# Interpolated into the query below, so it must stay a literal we control.
TABLES_DATABASE = "default"


class NodeVersionInfo(TypedDict):
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
    query_node_versions: Sequence[NodeVersionInfo]
    query_node_error: str | None
    storage_node_versions: Sequence[NodeVersionInfo]
    storage_node_error: str | None
    # `tables` contains the tables in the `default` database on the configured
    # query endpoint. It is empty if the lookup failed, in which case `error`
    # explains why.
    tables: Sequence[str]
    error: str | None


def _describe_cluster(cluster: ClickhouseCluster) -> ClusterInfo:
    """
    The statically configured properties of a cluster. Does not touch ClickHouse.
    """
    return {
        "host": cluster.get_host(),
        "port": cluster.get_port(),
        "database": cluster.get_database(),
        "secure": cluster.get_secure(),
        "single_node": cluster.is_single_node(),
        "cluster_name": cluster.get_clickhouse_cluster_name(),
        "distributed_cluster_name": cluster.get_clickhouse_distributed_cluster_name(),
        "storage_sets": sorted(storage_set.value for storage_set in cluster.get_storage_set_keys()),
        "query_node_versions": [],
        "query_node_error": None,
        "storage_node_versions": [],
        "storage_node_error": None,
        "tables": [],
        "error": None,
    }


class _ClusterState(NamedTuple):
    query_node_versions: Sequence[NodeVersionInfo]
    query_node_error: str | None
    storage_node_versions: Sequence[NodeVersionInfo]
    storage_node_error: str | None
    tables: Sequence[str]
    error: str | None


def _query_node_version(cluster: ClickhouseCluster, node: ClickhouseNode) -> NodeVersionInfo:
    info: NodeVersionInfo = {
        "host": node.host_name,
        "port": node.port,
        "version": None,
        "error": None,
    }
    try:
        results = (
            get_ro_cluster_node_connection(cluster, node, ClickhouseClientSettings.QUERY)
            .execute("SELECT version()")
            .results
        )
        if not results:
            raise Exception("ClickHouse returned no version")
        info["version"] = str(results[0][0])
    except Exception as e:
        logger.warning(str(e), cluster=str(cluster), node=str(node))
        info["error"] = str(e)
    return info


def _get_node_versions(
    cluster: ClickhouseCluster, nodes: Sequence[ClickhouseNode]
) -> Sequence[NodeVersionInfo]:
    if not nodes:
        return []
    if len(nodes) == 1:
        return [_query_node_version(cluster, nodes[0])]

    with ThreadPoolExecutor(max_workers=min(MAX_CONCURRENT_NODE_QUERIES, len(nodes))) as executor:
        return list(executor.map(lambda node: _query_node_version(cluster, node), nodes))


def _get_tables(cluster: ClickhouseCluster) -> Sequence[str]:
    results = (
        get_ro_cluster_node_connection(
            cluster, cluster.get_query_node(), ClickhouseClientSettings.QUERY
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


def _get_cluster_state(cluster: ClickhouseCluster, deadline: float | None = None) -> _ClusterState:
    """Query query nodes, storage nodes, and tables through validated admin pools."""
    if deadline is None:
        deadline = time.monotonic() + CLUSTER_QUERY_TIMEOUT
    query_node_error = None
    storage_node_error = None
    if cluster.is_single_node():
        query_nodes: Sequence[ClickhouseNode] = [cluster.get_query_node()]
        storage_nodes: Sequence[ClickhouseNode] = query_nodes
    else:
        try:
            query_nodes = cluster.get_distributed_nodes() or [cluster.get_query_node()]
        except Exception as e:
            # Topology discovery failed, but the configured query endpoint is
            # still known statically and can serve version/table lookups.
            logger.warning(str(e), cluster=str(cluster), node_role="query")
            query_nodes = [cluster.get_query_node()]
            query_node_error = str(e)

        try:
            storage_nodes = cluster.get_local_nodes()
        except Exception as e:
            logger.warning(str(e), cluster=str(cluster), node_role="storage")
            storage_nodes = []
            storage_node_error = str(e)

    # Deduplicate connection work when the same host appears in both roles or
    # when single-node clusters reuse one endpoint for both columns.
    unique_nodes: list[ClickhouseNode] = []
    seen: set[tuple[str, int]] = set()
    for node in [*query_nodes, *storage_nodes]:
        key = (node.host_name, node.port)
        if key in seen:
            continue
        seen.add(key)
        unique_nodes.append(node)

    node_versions: Sequence[NodeVersionInfo] = []
    tables: Sequence[str] = []
    error = None
    versions_failed = False
    # Run version lookups and the tables query concurrently. Both share the
    # outer cluster deadline, so finishing independent work in parallel keeps
    # one slow path from starving the other. Isolate each future so a failure
    # in one does not discard results already produced by the other.
    executor = ThreadPoolExecutor(max_workers=2)
    try:
        versions_future = executor.submit(_get_node_versions, cluster, unique_nodes)
        tables_future = executor.submit(_get_tables, cluster)
        try:
            node_versions = versions_future.result(timeout=max(0, deadline - time.monotonic()))
        except Exception as e:
            version_error = (
                f"Timed out after {CLUSTER_QUERY_TIMEOUT}s"
                if isinstance(e, FutureTimeoutError)
                else str(e)
            )
            logger.warning(version_error, cluster=str(cluster), lookup="versions")
            versions_failed = True
            if query_node_error is None:
                query_node_error = version_error
            if storage_node_error is None:
                storage_node_error = version_error
        try:
            tables = tables_future.result(timeout=max(0, deadline - time.monotonic()))
        except Exception as e:
            error = (
                f"Timed out after {CLUSTER_QUERY_TIMEOUT}s"
                if isinstance(e, FutureTimeoutError)
                else str(e)
            )
            logger.warning(error, cluster=str(cluster), lookup="tables")
    finally:
        executor.shutdown(wait=False, cancel_futures=True)

    if versions_failed:
        query_node_versions: Sequence[NodeVersionInfo] = []
        storage_node_versions: Sequence[NodeVersionInfo] = []
    else:
        versions_by_key = {(info["host"], info["port"]): info for info in node_versions}
        query_node_versions = [versions_by_key[(node.host_name, node.port)] for node in query_nodes]
        storage_node_versions = (
            query_node_versions
            if cluster.is_single_node()
            else [versions_by_key[(node.host_name, node.port)] for node in storage_nodes]
        )

    return _ClusterState(
        query_node_versions,
        query_node_error,
        storage_node_versions,
        storage_node_error,
        tables,
        error,
    )


def get_cluster_info() -> Sequence[ClusterInfo]:
    """
    Describe every configured cluster, including versions for each query and
    storage node and tables on its configured query endpoint.

    A cluster that cannot be reached still shows up, with the reason in its
    `error` field, so that the page always lists the full set of clusters.
    """
    if not CLUSTERS:
        return []

    cluster_info = [_describe_cluster(cluster) for cluster in CLUSTERS]

    executor = ThreadPoolExecutor(max_workers=min(MAX_CONCURRENT_CLUSTER_QUERIES, len(CLUSTERS)))
    try:
        # The lookups run concurrently, so the timeout is a deadline for all of
        # them rather than a per cluster budget. Inner lookups share this same
        # deadline and stop holding an outer worker when it expires.
        deadline = time.monotonic() + CLUSTER_QUERY_TIMEOUT
        states = [executor.submit(_get_cluster_state, cluster, deadline) for cluster in CLUSTERS]
        for cluster, info, state in zip(CLUSTERS, cluster_info, states, strict=True):
            try:
                (
                    info["query_node_versions"],
                    info["query_node_error"],
                    info["storage_node_versions"],
                    info["storage_node_error"],
                    info["tables"],
                    info["error"],
                ) = state.result(timeout=max(0, deadline - time.monotonic()))
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
        # Do not wait: a query that outlived the timeout above must not hold up
        # the response. Its thread ends on its own once ClickHouse (or the
        # driver's own timeout) lets go.
        executor.shutdown(wait=False, cancel_futures=True)

    return cluster_info
