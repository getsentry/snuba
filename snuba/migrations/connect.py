import re
import time
from typing import List, Sequence, Tuple

import structlog
from packaging import version

from snuba.clickhouse.native import ClickhousePool
from snuba.clusters.cluster import (
    ClickhouseClientSettings,
    ClickhouseCluster,
    ClickhouseNode,
    UndefinedClickhouseCluster,
    get_cluster,
)
from snuba.clusters.storage_sets import DEV_STORAGE_SETS
from snuba.datasets.readiness_state import ReadinessState
from snuba.datasets.storages.factory import get_all_storage_keys, get_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.migrations.clickhouse import (
    CLICKHOUSE_SERVER_MAX_VERSION,
    CLICKHOUSE_SERVER_MIN_VERSION,
)
from snuba.migrations.errors import InactiveClickhouseReplica, InvalidClickhouseVersion
from snuba.migrations.groups import (
    MigrationGroup,
    get_group_readiness_state_from_storage_set,
    get_storage_set_keys,
)
from snuba.settings import ENABLE_DEV_FEATURES
from snuba.utils.types import ColumnStatesMapType

logger = structlog.get_logger().bind(module=__name__)


def get_clickhouse_clusters_for_migration_group(
    migration_group: MigrationGroup,
) -> List[ClickhouseCluster]:
    storage_set_keys = get_storage_set_keys(migration_group)
    return list({get_cluster(storage_set_key) for storage_set_key in storage_set_keys})


def get_storages_for_readiness_states(
    readiness_states: Sequence[ReadinessState], storage_keys: Sequence[StorageKey]
):
    pass


def get_clusters_for_readiness_states(
    readiness_states: Sequence[ReadinessState], clusters: Sequence[ClickhouseCluster]
) -> Sequence[ClickhouseCluster]:
    """Given a set of clusters, return just the ones that serve storage_sets corresponding to the provided readiness states
    Storage sets do not have readiness states but the migration groups those storage sets are related to do

    E.g.

    # Get all the configured clusters which have partial or complete readiness states located on them
    from snuba.clusters.cluster import CLUSTERS

    get_clusters_for_readiness_states(
        [ReadinessState.PARTIAL, readiness_state.COMPLETE],
        CLUSTERS
    )

    """
    res = []
    for cluster in clusters:
        storage_sets = cluster.get_storage_set_keys()
        cluster_readiness_states = {
            get_group_readiness_state_from_storage_set(storage_set_key)
            for storage_set_key in storage_sets
        }
        if set(readiness_states).intersection(cluster_readiness_states):
            res.append(cluster)
    return res


def check_clickhouse_connections(
    clusters: Sequence[ClickhouseCluster],
) -> None:
    """
    Ensure that we can establish a connection with every cluster.
    """
    attempts = 0

    for cluster in clusters:
        clickhouse = cluster.get_query_connection(ClickhouseClientSettings.MIGRATE)

        while True:
            try:
                logger.debug(
                    "Attempting to connect to Clickhouse cluster %s (attempt %d)",
                    cluster,
                    attempts,
                )
                check_clickhouse(clickhouse)
                break
            except InvalidClickhouseVersion as e:
                logger.error(e)
                raise
            except Exception as e:
                logger.error(
                    "Connection to Clickhouse cluster %s failed (attempt %d)",
                    cluster,
                    attempts,
                    exc_info=e,
                )
                attempts += 1
                if attempts == 60:
                    raise
                time.sleep(1)


def check_clickhouse(clickhouse: ClickhousePool) -> None:
    """
    Checks that the clickhouse version is at least the min version and at most the max version
    """
    ver = clickhouse.execute("SELECT version()").results[0][0]
    ver = re.search("(\d+.\d+.\d+.\d+)", ver)
    if ver is None or version.parse(ver.group()) < version.parse(
        CLICKHOUSE_SERVER_MIN_VERSION
    ):
        raise InvalidClickhouseVersion(
            f"Snuba requires minimum Clickhouse version {CLICKHOUSE_SERVER_MIN_VERSION} ({clickhouse.host}:{clickhouse.port} - {version.parse(ver.group())})"
        )

    if version.parse(ver.group()) > version.parse(CLICKHOUSE_SERVER_MAX_VERSION):
        logger.warning(
            f"Snuba has only been tested on Clickhouse versions up to {CLICKHOUSE_SERVER_MAX_VERSION} ({clickhouse.host}:{clickhouse.port} - {version.parse(ver.group())}). Higher versions might not be supported."
        )


def _get_all_storage_keys() -> Sequence[StorageKey]:
    """
    Returns all storage keys that are not part of a dev storage set.
    """
    return [
        storage_key
        for storage_key in sorted(
            get_all_storage_keys(), key=lambda storage_key: storage_key.value
        )
        if get_storage(storage_key).get_storage_set_key() not in DEV_STORAGE_SETS
        or ENABLE_DEV_FEATURES
    ]


def _get_all_nodes_for_storage(
    storage_key: StorageKey,
) -> Tuple[Sequence[ClickhouseNode], Sequence[ClickhouseNode], ClickhouseNode]:
    """
    Returns all nodes for a given storage key.
    """
    storage = get_storage(storage_key)
    cluster = storage.get_cluster()
    query_node = cluster.get_query_node()
    if storage_key == StorageKey.DISCOVER:
        local_nodes: Sequence[ClickhouseNode] = []
        distributed_nodes: Sequence[ClickhouseNode] = []
    else:
        local_nodes = cluster.get_local_nodes()
        distributed_nodes = cluster.get_distributed_nodes()

    return (local_nodes, distributed_nodes, query_node)


def check_for_inactive_replicas(storage_keys: List[StorageKey]) -> None:
    """
    Checks for inactive replicas and raise InactiveClickhouseReplica if any are found.
    """

    # storage_keys = _get_all_storage_keys()

    checked_nodes = set()
    inactive_replica_info = []
    for storage_key in storage_keys:
        try:
            local_nodes, distributed_nodes, query_node = _get_all_nodes_for_storage(
                storage_key
            )
            storage = get_storage(storage_key)
            cluster = storage.get_cluster()
        except UndefinedClickhouseCluster:
            continue

        for node in (*local_nodes, *distributed_nodes, query_node):
            if (node.host_name, node.port) in checked_nodes:
                continue
            checked_nodes.add((node.host_name, node.port))

            conn = cluster.get_node_connection(ClickhouseClientSettings.MIGRATE, node)
            tables_with_inactive = conn.execute(
                f"SELECT table, total_replicas, active_replicas FROM system.replicas "
                f"WHERE active_replicas < total_replicas AND database ='{conn.database}'",
            ).results

            for table, total_replicas, active_replicas in tables_with_inactive:
                inactive_replica_info.append(
                    f"Storage {storage_key.value} has inactive replicas for table {table} "
                    f"with {active_replicas} out of {total_replicas} replicas active."
                )

    if inactive_replica_info:
        raise InactiveClickhouseReplica("\n".join(sorted(set(inactive_replica_info))))


def get_column_states() -> ColumnStatesMapType:
    """
    For every node in the cluster, get the current type of the columns.
    Passing on this information to the migrations will allow them to
    check for dangerous migrations
    """
    storage_keys = _get_all_storage_keys()
    column_states: ColumnStatesMapType = {}
    checked_nodes = set()
    for storage_key in storage_keys:
        try:
            local_nodes, distributed_nodes, query_node = _get_all_nodes_for_storage(
                storage_key
            )
            storage = get_storage(storage_key)
            cluster = storage.get_cluster()
        except UndefinedClickhouseCluster:
            continue
        for node in (*local_nodes, *distributed_nodes, query_node):
            if (node.host_name, node.port) in checked_nodes:
                continue
            checked_nodes.add((node.host_name, node.port))

            conn = cluster.get_node_connection(ClickhouseClientSettings.MIGRATE, node)
            column_types = conn.execute(
                "SELECT table, name, type FROM system.columns "
                f"WHERE database='{conn.database}'",
            ).results

            for row in column_types:
                table, col_name, type = row
                column_states[(node.host_name, node.port, table, col_name)] = type

    return column_states
