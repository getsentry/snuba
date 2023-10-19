from typing import Set, Tuple

from snuba.clusters.cluster import ClickhouseClientSettings, get_cluster
from snuba.clusters.storage_sets import StorageSetKey

ClickhouseVersion = Tuple[int, int]


def get_clickhouse_version_for_storage_set(
    storage_set: StorageSetKey,
) -> ClickhouseVersion:
    """
    Determine the clickhouse version for a storage set. Assumes (and verifies)
    that all local nodes have the same version for simplicity.
    """

    cluster = get_cluster(storage_set)
    versions: Set[ClickhouseVersion] = set()
    for node in cluster.get_local_nodes():
        connection = cluster.get_node_connection(ClickhouseClientSettings.MIGRATE, node)
        ver = connection.execute("SELECT version()").results[0][0]

        major, minor, *_ = ver.split(".")
        versions.add((int(major), int(minor)))

    if len(versions) != 1:
        raise RuntimeError(
            f"found multiple clickhouse versions in local nodes of storage set {storage_set}: {versions}"
        )

    return versions.pop()


_CLICKHOUSE_SETTINGS_SUPPORTED = {
    # https://github.com/ClickHouse/ClickHouse/pull/12433#issuecomment-685987783
    "allow_nullable_key": (20, 7),
}


def supports_setting(clickhouse_version: ClickhouseVersion, setting: str) -> bool:
    """
    For a given setting, determine whether the given clickhouse version
    supports it.
    """
    return _CLICKHOUSE_SETTINGS_SUPPORTED[setting] <= clickhouse_version
