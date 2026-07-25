from __future__ import annotations

import time
from collections.abc import Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FutureTimeoutError
from typing import NamedTuple, TypedDict

import structlog

from snuba import settings
from snuba.admin.clickhouse.common import get_ro_query_node_connection
from snuba.clusters.cluster import (
    CLUSTERS,
    ClickhouseClientSettings,
    ClickhouseCluster,
    UndefinedClickhouseCluster,
)
from snuba.clusters.storage_sets import DEV_STORAGE_SETS
from snuba.datasets.storages.factory import get_all_storage_keys, get_storage
from snuba.datasets.storages.storage_key import StorageKey

logger = structlog.get_logger().bind(module=__name__)

# One lookup per cluster, run concurrently so that a single slow or unreachable
# cluster does not hold up the whole page.
MAX_CONCURRENT_CLUSTER_QUERIES = 32
CLUSTER_QUERY_TIMEOUT = 30

# Tables are only listed for this database, the one Snuba's own tables live in.
# Interpolated into the query below, so it must stay a literal we control.
TABLES_DATABASE = "default"


class ClusterInfo(TypedDict):
    # The cluster's configured query endpoint (an actual node or a proxy).
    host: str
    port: int
    http_port: int
    database: str
    secure: bool
    single_node: bool
    # The ClickHouse cluster names, as they appear in system.clusters. These are
    # only set for multi node clusters.
    cluster_name: str | None
    distributed_cluster_name: str | None
    storage_sets: Sequence[str]
    # `version` is the value of `version()` on the query endpoint, and `tables`
    # the tables that exist in the `default` database on it. They are None/empty
    # if the lookup failed, in which case `error` explains why.
    version: str | None
    tables: Sequence[str]
    error: str | None


def _storage_name_by_cluster() -> Mapping[int, str]:
    """
    Map each cluster (by object identity) to the name of one of the storages
    registered on it.

    The admin read-only connection helpers are keyed by storage name, so a
    cluster can only be queried through one of its storages.
    """

    def preference(storage_key: StorageKey) -> tuple[bool, str]:
        # Discover does not follow the typical cluster pattern: it is not a
        # single node but does not belong to a cluster either, so node lookups
        # (which the connection helpers validate against) can fail for it. Only
        # fall back to it if it is the sole storage on a cluster.
        return (storage_key == StorageKey.DISCOVER, storage_key.value)

    storage_names: dict[int, str] = {}
    for storage_key in sorted(get_all_storage_keys(), key=preference):
        try:
            storage = get_storage(storage_key)
            if (
                storage.get_storage_set_key() in DEV_STORAGE_SETS
                and not settings.ENABLE_DEV_FEATURES
            ):
                continue
            cluster = storage.get_cluster()
        except (AssertionError, KeyError, UndefinedClickhouseCluster) as e:
            logger.warning(str(e), storage_key=storage_key.value)
            continue
        storage_names.setdefault(id(cluster), storage_key.value)

    return storage_names


def _describe_cluster(cluster: ClickhouseCluster) -> ClusterInfo:
    """
    The statically configured properties of a cluster. Does not touch ClickHouse.
    """
    return {
        "host": cluster.get_host(),
        "port": cluster.get_port(),
        "http_port": cluster.get_http_port(),
        "database": cluster.get_database(),
        "secure": cluster.get_secure(),
        "single_node": cluster.is_single_node(),
        "cluster_name": cluster.get_clickhouse_cluster_name(),
        "distributed_cluster_name": cluster.get_clickhouse_distributed_cluster_name(),
        "storage_sets": sorted(storage_set.value for storage_set in cluster.get_storage_set_keys()),
        "version": None,
        "tables": [],
        "error": None,
    }


class _ClusterState(NamedTuple):
    version: str
    tables: Sequence[str]


def _get_cluster_state(storage_name: str | None) -> _ClusterState:
    """
    Query the query endpoint of the cluster `storage_name` is registered on for
    the ClickHouse version it runs and the tables it holds in the
    `TABLES_DATABASE` database.
    """
    if storage_name is None:
        raise Exception("No storage is registered on this cluster")

    connection = get_ro_query_node_connection(storage_name, ClickhouseClientSettings.QUERY)
    # One round trip: the aggregate accumulates the table names into a single
    # sorted array, so the whole cluster is described by one row. It sits in a
    # scalar subquery to keep the outer SELECT aggregate free -- version() next
    # to a bare aggregate is at the mercy of constant folding -- and a scalar
    # subquery still yields a row (an empty array) on a database with no tables.
    results = connection.execute(
        f"""
        SELECT
            version() AS version,
            (
                SELECT arraySort(groupUniqArray(name))
                FROM system.tables
                WHERE database = '{TABLES_DATABASE}'
            ) AS tables
        """
    ).results
    if not results:
        raise Exception("ClickHouse returned no version")

    version, tables = results[0][0], results[0][1]
    return _ClusterState(str(version), [str(table) for table in tables])


def get_cluster_info() -> Sequence[ClusterInfo]:
    """
    Describe every cluster this Snuba deployment is configured with (the
    CLUSTERS setting), along with the ClickHouse version it is running and the
    tables it holds in the `default` database.

    A cluster that cannot be reached still shows up, with the reason in its
    `error` field, so that the page always lists the full set of clusters.
    """
    if not CLUSTERS:
        return []

    storage_names = _storage_name_by_cluster()
    cluster_info = [_describe_cluster(cluster) for cluster in CLUSTERS]

    executor = ThreadPoolExecutor(max_workers=min(MAX_CONCURRENT_CLUSTER_QUERIES, len(CLUSTERS)))
    try:
        states = [
            executor.submit(_get_cluster_state, storage_names.get(id(cluster)))
            for cluster in CLUSTERS
        ]
        # The lookups run concurrently, so the timeout is a deadline for all of
        # them rather than a per cluster budget.
        deadline = time.monotonic() + CLUSTER_QUERY_TIMEOUT
        for cluster, info, state in zip(CLUSTERS, cluster_info, states, strict=True):
            try:
                info["version"], info["tables"] = state.result(
                    timeout=max(0, deadline - time.monotonic())
                )
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
