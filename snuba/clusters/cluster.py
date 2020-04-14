from typing import MutableMapping, Set

from snuba import settings
from snuba.clickhouse.native import ClickhousePool, NativeDriverReader
from snuba.clickhouse.query import ClickhouseQuery
from snuba.clusters.storage_sets import StorageSetKey, STORAGE_SETS
from snuba.datasets.storages import StorageKey
from snuba.reader import Reader


class Cluster:
    """
    A cluster is responsible for managing a collection of database nodes.

    Clusters are configurable, and will be instantiated based on user defined settings.

    Each storage must be mapped to a cluster via a storage set, which defines
    the storages that must be located on the same cluster.

    A cluster provides a reader and Clickhouse connections that are shared by all
    storages located on the cluster.

    In future, clusters will also be reponsible for co-ordinating commands that
    need to be run on multiple hosts that are colocated within the same cluster -
    e.g. bootstrap, migration, cleanup, optimize.
    """

    def __init__(self, host: str, port: int, http_port: int, storage_sets: Set[str]):
        self.__storage_sets = storage_sets
        self.__clickhouse_rw = ClickhousePool(host, port)
        self.__clickhouse_ro = ClickhousePool(
            host, port, client_settings={"readonly": True},
        )
        self.__reader: Reader[ClickhouseQuery] = NativeDriverReader(
            self.__clickhouse_ro
        )

    def get_storage_sets(self) -> Set[str]:
        return self.__storage_sets

    def get_clickhouse_rw(self) -> ClickhousePool:
        return self.__clickhouse_rw

    def get_clickhouse_ro(self) -> ClickhousePool:
        return self.__clickhouse_ro

    def get_reader(self) -> Reader[ClickhouseQuery]:
        return self.__reader


CLUSTERS = [
    Cluster(
        host=cluster["host"],
        port=cluster["port"],
        http_port=cluster["http_port"],
        storage_sets=cluster["storage_sets"],
    )
    for cluster in settings.CLUSTERS
]

_registered_storage_sets = [
    storage_set for cluster in CLUSTERS for storage_set in cluster.get_storage_sets()
]
assert len(_registered_storage_sets) == len(
    (set(_registered_storage_sets))
), "Storage set registered to more than one cluster"

assert set([s.value for s in StorageSetKey]) == set(
    _registered_storage_sets
), "All storage sets must be assigned to a cluster"

# Map all storages to clusters via storage sets
_STORAGE_CLUSTER_MAP: MutableMapping[StorageKey, Cluster] = {}
for cluster in CLUSTERS:
    for storage_set_key in cluster.get_storage_sets():
        for storage_key in STORAGE_SETS[StorageSetKey(storage_set_key)]:
            _STORAGE_CLUSTER_MAP[storage_key] = cluster


def get_cluster(storage_key: StorageKey) -> Cluster:
    return _STORAGE_CLUSTER_MAP[storage_key]
