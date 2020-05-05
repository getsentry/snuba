from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Generic, Optional, Sequence, Set

from snuba import settings
from snuba.clickhouse.escaping import escape_string
from snuba.clickhouse.native import ClickhousePool, NativeDriverReader
from snuba.clickhouse.sql import SqlQuery
from snuba.clusters.storage_sets import StorageSetKey
from snuba.reader import Reader, TQuery


@dataclass(frozen=True)
class ClickhouseHost:
    host_name: str
    port: int
    shard: Optional[int] = None
    replica: Optional[int] = None


class Cluster(ABC, Generic[TQuery]):
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

    def get_storage_set_keys(self) -> Set[StorageSetKey]:
        return {StorageSetKey(storage_set) for storage_set in self.__storage_sets}

    @abstractmethod
    def get_reader(self) -> Reader[TQuery]:
        raise NotImplementedError


class ClickhouseCluster(Cluster[SqlQuery]):
    """
    ClickhouseCluster provides a reader and Clickhouse connections that are shared by all
    storages located on the cluster
    """

    def __init__(
        self,
        host: str,
        port: int,
        http_port: int,
        storage_sets: Set[str],
        is_distributed: bool,
        # The cluster name if is_distributed is set to True
        cluster_name: Optional[str] = None,
    ):
        super().__init__(storage_sets)
        if is_distributed:
            assert cluster_name
        self.__host = host
        self.__port = port
        self.__is_distributed = is_distributed
        self.__clickhouse_rw = ClickhousePool(host, port)
        self.__clickhouse_ro = ClickhousePool(
            host, port, client_settings={"readonly": True},
        )
        self.__reader = NativeDriverReader(self.__clickhouse_ro)
        self.__cluster_name = cluster_name

    def __str__(self) -> str:
        return f"{self.__host}:{self.__port}"

    def get_clickhouse_rw(self) -> ClickhousePool:
        return self.__clickhouse_rw

    def get_clickhouse_ro(self) -> ClickhousePool:
        return self.__clickhouse_ro

    def get_reader(self) -> Reader[SqlQuery]:
        return self.__reader

    def get_nodes(self) -> Sequence[ClickhouseHost]:
        if self.__is_distributed:
            # Get the nodes from system.clusters
            return [
                ClickhouseHost(*host)
                for host in self.get_clickhouse_ro().execute(
                    f"select host_name, port, shard_num, replica_num from system.clusters where cluster={escape_string(self.__cluster_name)}"
                )
            ]
        else:
            return [ClickhouseHost(self.__host, self.__port)]


CLUSTERS = [
    ClickhouseCluster(
        host=cluster["host"],
        port=cluster["port"],
        http_port=cluster["http_port"],
        storage_sets=cluster["storage_sets"],
        is_distributed=cluster["distributed"],
        cluster_name=cluster["cluster_name"] if "cluster_name" in cluster else None,
    )
    for cluster in settings.CLUSTERS
]

_registered_storage_sets = [
    storage_set
    for cluster in CLUSTERS
    for storage_set in cluster.get_storage_set_keys()
]

_unique_registered_storage_sets = set(_registered_storage_sets)

assert len(_registered_storage_sets) == len(
    _unique_registered_storage_sets
), "Storage set registered to more than one cluster"

assert (
    set(StorageSetKey) == _unique_registered_storage_sets
), "All storage sets must be assigned to a cluster"

# Map all storages to clusters via storage sets
_STORAGE_SET_CLUSTER_MAP = {
    storage_set: cluster
    for cluster in CLUSTERS
    for storage_set in cluster.get_storage_set_keys()
}


def get_cluster(storage_set_key: StorageSetKey) -> ClickhouseCluster:
    return _STORAGE_SET_CLUSTER_MAP[storage_set_key]
