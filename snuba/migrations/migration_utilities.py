from typing import Tuple

from snuba.clusters.cluster import ClickhouseClientSettings, get_cluster
from snuba.clusters.storage_sets import StorageSetKey

ClickhouseVersion = Tuple[int, int]


def get_clickhouse_version_for_storage_set(
    storage_set: StorageSetKey,
) -> ClickhouseVersion:
    cluster = get_cluster(storage_set)
    versions = set()
    for node in cluster.get_local_nodes():
        connection = cluster.get_node_connection(ClickhouseClientSettings.MIGRATE, node)
        ver = connection.execute("SELECT version()").results[0][0]

        versions.add(tuple(map(int, ver.split(".")[:2])))

    if len(versions) != 1:
        raise RuntimeError(
            f"found multiple clickhouse versions in local nodes of storage set {storage_set}: {versions}"
        )

    (ver,) = versions
    return ver


_CLICKHOUSE_SETTINGS_SUPPORTED = {
    # https://github.com/ClickHouse/ClickHouse/pull/12433#issuecomment-685987783
    "allow_nullable_key": (20, 7),
}


def supports_setting(clickhouse_version: ClickhouseVersion, setting: str) -> bool:
    return _CLICKHOUSE_SETTINGS_SUPPORTED[setting] <= clickhouse_version
