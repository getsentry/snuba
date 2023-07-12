import re
import time
from typing import Sequence

import structlog
from packaging import version

from snuba.clickhouse.native import ClickhousePool
from snuba.clusters.cluster import (
    CLUSTERS,
    ClickhouseClientSettings,
    ClickhouseCluster,
    ClickhouseNode,
    UndefinedClickhouseCluster,
)
from snuba.clusters.storage_sets import DEV_STORAGE_SETS
from snuba.datasets.storages.factory import get_all_storage_keys, get_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.migrations.clickhouse import (
    CLICKHOUSE_SERVER_MAX_VERSION,
    CLICKHOUSE_SERVER_MIN_VERSION,
)
from snuba.migrations.errors import InactiveClickhouseReplica, InvalidClickhouseVersion
from snuba.settings import CLICKHOUSE_STARTUP_CHECK_RETRIES, ENABLE_DEV_FEATURES

logger = structlog.get_logger().bind(module=__name__)


def check_clickhouse_connections(
    clusters: Sequence[ClickhouseCluster] = CLUSTERS,
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
                if attempts == CLICKHOUSE_STARTUP_CHECK_RETRIES:
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


def check_for_inactive_replicas() -> None:
    """
    Checks for inactive replicas and raise InactiveClickhouseReplica if any are found.
    """

    storage_keys = [
        storage_key
        for storage_key in sorted(
            get_all_storage_keys(), key=lambda storage_key: storage_key.value
        )
        if get_storage(storage_key).get_storage_set_key() not in DEV_STORAGE_SETS
        or ENABLE_DEV_FEATURES
    ]

    checked_nodes = set()
    inactive_replica_info = []
    for storage_key in storage_keys:
        storage = get_storage(storage_key)
        try:
            cluster = storage.get_cluster()
        except UndefinedClickhouseCluster:
            continue

        query_node = cluster.get_query_node()
        if storage_key == StorageKey.DISCOVER:
            local_nodes: Sequence[ClickhouseNode] = []
            distributed_nodes: Sequence[ClickhouseNode] = []
        else:
            local_nodes = cluster.get_local_nodes()
            distributed_nodes = cluster.get_distributed_nodes()

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
