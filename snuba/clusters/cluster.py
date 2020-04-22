from abc import ABC, abstractmethod
from typing import MutableMapping, Set

from snuba import settings
from snuba.clickhouse.native import ClickhousePool, NativeDriverReader
from snuba.clickhouse.query import ClickhouseQuery
from snuba.clusters.storage_sets import StorageSetKey
from snuba.reader import Reader


class Cluster(ABC):
    """
    A cluster is responsible for managing a collection of database nodes.

    Clusters are configurable, and will be instantiated based on user defined settings.

    Each storage must be mapped to a cluster via a storage set, which defines
    the storages that must be located on the same cluster.

    In future, clusters will also be responsible for co-ordinating commands that
    need to be run on multiple hosts that are colocated within the same cluster.
    The cluster will expose methods for:
        - bootstrap
        - migrate
        - cleanup
        - optimize
    """

    def __init__(self, storage_sets: Set[str]):
        self.__storage_sets = storage_sets

    def get_storage_sets(self) -> Set[StorageSetKey]:
        return {StorageSetKey(storage_set) for storage_set in self.__storage_sets}

    @abstractmethod
    def get_reader(self) -> Reader[ClickhouseQuery]:
        raise NotImplementedError


class ClickhouseCluster(Cluster):
    """
    ClickhouseCluster provides a reader and Clickhouse connections that are shared by all
    storages located on the cluster
    """

    def __init__(self, host: str, port: int, http_port: int, storage_sets: Set[str]):
        self.__host = host
        self.__port = port
        self.__clickhouse_rw = ClickhousePool(host, port)
        self.__clickhouse_ro = ClickhousePool(
            host, port, client_settings={"readonly": True},
        )
        self.__reader: Reader[ClickhouseQuery] = NativeDriverReader(
            self.__clickhouse_ro
        )
        super().__init__(storage_sets)

    def __str__(self) -> str:
        return f"{self.__host}:{self.__port}"

    def get_clickhouse_rw(self) -> ClickhousePool:
        return self.__clickhouse_rw

    def get_clickhouse_ro(self) -> ClickhousePool:
        return self.__clickhouse_ro

    def get_reader(self) -> Reader[ClickhouseQuery]:
        return self.__reader


CLUSTERS = [
    ClickhouseCluster(
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

_unique_registered_storage_sets = set(_registered_storage_sets)

assert len(_registered_storage_sets) == len(
    _unique_registered_storage_sets
), "Storage set registered to more than one cluster"

assert (
    set(StorageSetKey) == _unique_registered_storage_sets
), "All storage sets must be assigned to a cluster"

# Map all storages to clusters via storage sets
_STORAGE_SET_CLUSTER_MAP: MutableMapping[StorageSetKey, ClickhouseCluster] = {}
for cluster in CLUSTERS:
    for storage_set_key in cluster.get_storage_sets():
        _STORAGE_SET_CLUSTER_MAP[storage_set_key] = cluster


def get_cluster(storage_set_key: StorageSetKey) -> ClickhouseCluster:
    return _STORAGE_SET_CLUSTER_MAP[storage_set_key]
