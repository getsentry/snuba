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


def _single_node_target_key(cluster: ClickhouseCluster) -> str:
    """Identify a single-node endpoint by the settings the admin RO pool uses.

    `get_ro_cluster_node_connection` always authenticates with the global
    readonly credentials. It still takes database and TLS options from the
    cluster object, so those fields must participate in dedupe. App-level
    cluster credentials do not.
    """
    host = cluster.get_host()
    port = cluster.get_port()
    database = cluster.get_database()
    secure = cluster.get_secure()
    ca_certs = cluster.get_ca_certs() or ""
    verify = cluster.get_verify()
    return f"{host}:{port}:{database}:{secure}:{ca_certs}:{verify}"


def _cluster_targets() -> Sequence[_ClusterTarget]:
    """Return one target per unique ClickHouse cluster endpoint/name."""
    targets: dict[str, _ClusterTarget] = {}
    for cluster in CLUSTERS:
        storage_sets = {storage_set.value for storage_set in cluster.get_storage_set_keys()}
        if cluster.is_single_node():
            # Single-node configs have no ClickHouse cluster name. Key by the
            # admin RO pool identity so differing DB/TLS settings do not collapse.
            entries = [(_single_node_target_key(cluster), "single node", True)]
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


def _query_versions(target: _ClusterTarget) -> Sequence[str]:
    query_node = target.cluster.get_query_node()
    connection = get_ro_cluster_node_connection(
        target.cluster,
        query_node,
        ClickhouseClientSettings.QUERY,
        known_nodes=[query_node],
    )
    # system.one always yields one row per replica, including replicas with no
    # tables in `default`.
    version_rows = connection.execute(
        f"""
        SELECT DISTINCT version() AS version
        FROM {_cluster_source(target, "system.one")}
        ORDER BY version
        """
    ).results
    return [str(row[0]) for row in version_rows]


def _query_tables(target: _ClusterTarget) -> Sequence[str]:
    query_node = target.cluster.get_query_node()
    connection = get_ro_cluster_node_connection(
        target.cluster,
        query_node,
        ClickhouseClientSettings.QUERY,
        known_nodes=[query_node],
    )
    table_rows = connection.execute(
        f"""
        SELECT arraySort(groupUniqArray(name)) AS tables
        FROM {_cluster_source(target, "system.tables")}
        WHERE database = '{TABLES_DATABASE}'
        """
    ).results
    return sorted({str(table) for row in table_rows for table in row[0]})


def _get_cluster_state(target: _ClusterTarget, deadline: float | None = None) -> _ClusterState:
    """Fetch distinct versions and default-database tables for one cluster.

    Version and table lookups share the outer deadline and run concurrently so
    one slow path cannot starve the other past the parent timeout.
    """
    if deadline is None:
        deadline = time.monotonic() + CLUSTER_QUERY_TIMEOUT

    versions: Sequence[str] = ()
    tables: Sequence[str] = ()
    error: str | None = None
    executor = ThreadPoolExecutor(max_workers=2)
    try:
        versions_future = executor.submit(_query_versions, target)
        tables_future = executor.submit(_query_tables, target)
        try:
            versions = versions_future.result(timeout=max(0, deadline - time.monotonic()))
        except Exception as e:
            error = (
                f"Timed out after {CLUSTER_QUERY_TIMEOUT}s"
                if isinstance(e, FutureTimeoutError)
                else str(e)
            )
            logger.warning(error, cluster=target.name, lookup="versions")
        try:
            tables = tables_future.result(timeout=max(0, deadline - time.monotonic()))
        except Exception as e:
            table_error = (
                f"Timed out after {CLUSTER_QUERY_TIMEOUT}s"
                if isinstance(e, FutureTimeoutError)
                else str(e)
            )
            logger.warning(table_error, cluster=target.name, lookup="tables")
            error = error or table_error
    finally:
        executor.shutdown(wait=False, cancel_futures=True)

    if error is not None:
        raise Exception(error)
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
        states = [executor.submit(_get_cluster_state, target, deadline) for target in targets]
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
        # Do not wait: a query that outlived the timeout above must not hold up
        # the response. Its thread ends on its own once ClickHouse (or the
        # driver's own timeout) lets go.
        executor.shutdown(wait=False, cancel_futures=True)

    return info
