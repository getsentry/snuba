from abc import ABC, abstractmethod
from enum import Enum
from typing import Any, Generic, Mapping, MutableMapping, Optional, Set, Tuple

from snuba import settings
from snuba.clickhouse.native import ClickhousePool, NativeDriverReader
from snuba.clickhouse.sql import SqlQuery
from snuba.clusters.storage_sets import StorageSetKey
from snuba.reader import Reader, TQuery


class ClickhouseClientSettings(Enum):
    CLEANUP: Mapping[str, Any] = {}
    INSERT: Mapping[str, Any] = {}
    MIGRATE: Mapping[str, Any] = {}
    OPTIMIZE: Mapping[str, Any] = {}
    QUERY = {"readonly": True}
    REPLACE = {
        # Replacing existing rows requires reconstructing the entire tuple for each
        # event (via a SELECT), which is a Hard Thing (TM) for columnstores to do. With
        # the default settings it's common for ClickHouse to go over the default max_memory_usage
        # of 10GB per query. Lowering the max_block_size reduces memory usage, and increasing the
        # max_memory_usage gives the query more breathing room.
        "max_block_size": settings.REPLACER_MAX_BLOCK_SIZE,
        "max_memory_usage": settings.REPLACER_MAX_MEMORY_USAGE,
        # Don't use up production cache for the count() queries.
        "use_uncompressed_cache": 0,
    }


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

    def __init__(self, host: str, port: int, http_port: int, storage_sets: Set[str]):
        super().__init__(storage_sets)
        self.__host = host
        self.__port = port
        self.__http_port = http_port
        self.__reader: Optional[Reader[SqlQuery]] = None
        self.__connection_cache: MutableMapping[
            Tuple[ClickhouseClientSettings, Optional[int]], ClickhousePool
        ] = {}

    def __str__(self) -> str:
        return f"{self.__host}:{self.__port}"

    def get_connection(
        self,
        client_settings: ClickhouseClientSettings,
        send_receive_timeout: Optional[int] = None,
    ) -> ClickhousePool:
        """
        Get a Clickhouse connection given client settings an a timeout.
        Reuse any connection to the cluster with the same settings and timeout values
        otherwise establish a new connection.
        """
        cache_key = (client_settings, send_receive_timeout)
        if cache_key not in self.__connection_cache:
            self.__connection_cache[cache_key] = ClickhousePool(
                self.__host,
                self.__port,
                client_settings=client_settings.value,
                send_receive_timeout=send_receive_timeout,
            )
        return self.__connection_cache[cache_key]

    def get_reader(self) -> Reader[SqlQuery]:
        if not self.__reader:
            self.__reader = NativeDriverReader(
                self.get_connection(ClickhouseClientSettings.QUERY)
            )
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
