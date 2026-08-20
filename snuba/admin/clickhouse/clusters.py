from __future__ import annotations

import time
from collections.abc import Sequence
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FutureTimeoutError
from typing import NamedTuple, TypedDict

import structlog

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


def _get_node_versions(
    cluster: ClickhouseCluster, nodes: Sequence[ClickhouseNode]
) -> Sequence[NodeVersionInfo]:
    versions: list[NodeVersionInfo] = []
    for node in nodes:
        info: NodeVersionInfo = {
            "host": node.host_name,
            "port": node.native_port,
            "version": None,
            "error": None,
        }
        try:
            results = cluster.get_node_connection(
                ClickhouseClientSettings.QUERY, node
            ).execute("SELECT version()").results
            if not results:
                raise Exception("ClickHouse returned no version")
            info["version"] = str(results[0][0])
        except Exception as e:
            logger.warning(str(e), cluster=str(cluster), node=str(node))
            info["error"] = str(e)
        versions.append(info)
    return versions


def _get_cluster_state(cluster: ClickhouseCluster) -> _ClusterState:
    """Query the configured query endpoint and each storage node directly."""
    query_node_error = None
    storage_node_error = None
    if cluster.is_single_node():
        query_node_versions = _get_node_versions(cluster, [cluster.get_query_node()])
        storage_node_versions = query_node_versions
    else:
        try:
            query_nodes = cluster.get_distributed_nodes() or [cluster.get_query_node()]
        except Exception as e:
            logger.warning(str(e), cluster=str(cluster), node_role="query")
            query_nodes = []
            query_node_error = str(e)
        query_node_versions = _get_node_versions(cluster, query_nodes)

        try:
            storage_node_versions = _get_node_versions(cluster, cluster.get_local_nodes())
        except Exception as e:
            logger.warning(str(e), cluster=str(cluster), node_role="storage")
            storage_node_versions = []
            storage_node_error = str(e)

    tables: Sequence[str] = []
    error = None
    try:
        results = cluster.get_query_connection(ClickhouseClientSettings.QUERY).execute(
            f"""
            SELECT arraySort(groupUniqArray(name))
            FROM system.tables
            WHERE database = '{TABLES_DATABASE}'
            """
        ).results
        if results:
            tables = [str(table) for table in results[0][0]]
    except Exception as e:
        logger.warning(str(e), cluster=str(cluster), lookup="tables")
        error = str(e)

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
        states = [executor.submit(_get_cluster_state, cluster) for cluster in CLUSTERS]
        # The lookups run concurrently, so the timeout is a deadline for all of
        # them rather than a per cluster budget.
        deadline = time.monotonic() + CLUSTER_QUERY_TIMEOUT
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
                info["error"] = f"Timed out after {CLUSTER_QUERY_TIMEOUT}s"
            except Exception as e:
                logger.warning(str(e), cluster=str(cluster))
                info["error"] = str(e)
    finally:
        # Do not wait: a query that outlived the timeout above must not hold up
        # the response. Its thread ends on its own once ClickHouse (or the
        # driver's own timeout) lets go.
        executor.shutdown(wait=False, cancel_futures=True)

    return cluster_info
