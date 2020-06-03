from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import (
    Any,
    Callable,
    Generic,
    Mapping,
    MutableMapping,
    NamedTuple,
    Optional,
    Sequence,
    Set,
    Tuple,
    TypeVar,
)

from snuba import settings
from snuba.clickhouse.escaping import escape_string
from snuba.clickhouse.http import HTTPBatchWriter
from snuba.clickhouse.native import ClickhousePool, NativeDriverReader
from snuba.clickhouse.sql import SqlQuery
from snuba.clusters.storage_sets import StorageSetKey
from snuba.reader import Reader, TQuery
from snuba.writer import BatchWriter, WriterTableRow


class ClickhouseClientSettingsType(NamedTuple):
    settings: Mapping[str, Any]
    timeout: Optional[int]


class ClickhouseClientSettings(Enum):
    CLEANUP = ClickhouseClientSettingsType({}, None)
    INSERT = ClickhouseClientSettingsType({}, None)
    MIGRATE = ClickhouseClientSettingsType({}, None)
    OPTIMIZE = ClickhouseClientSettingsType({}, 10000)
    QUERY = ClickhouseClientSettingsType({"readonly": True}, None)
    REPLACE = ClickhouseClientSettingsType(
        {
            # Replacing existing rows requires reconstructing the entire tuple for each
            # event (via a SELECT), which is a Hard Thing (TM) for columnstores to do. With
            # the default settings it's common for ClickHouse to go over the default max_memory_usage
            # of 10GB per query. Lowering the max_block_size reduces memory usage, and increasing the
            # max_memory_usage gives the query more breathing room.
            "max_block_size": settings.REPLACER_MAX_BLOCK_SIZE,
            "max_memory_usage": settings.REPLACER_MAX_MEMORY_USAGE,
            # Don't use up production cache for the count() queries.
            "use_uncompressed_cache": 0,
        },
        None,
    )


@dataclass(frozen=True)
class ClickhouseNode:
    host_name: str
    port: int
    shard: Optional[int] = None
    replica: Optional[int] = None

    def __str__(self) -> str:
        return f"{self.host_name}:{self.port}"


TWriterOptions = TypeVar("TWriterOptions")


class Cluster(ABC, Generic[TQuery, TWriterOptions]):
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

    @abstractmethod
    def get_writer(
        self,
        table_name: str,
        encoder: Callable[[WriterTableRow], bytes],
        options: TWriterOptions,
        chunk_size: Optional[int],
    ) -> BatchWriter:
        raise NotImplementedError


ClickhouseWriterOptions = Optional[Mapping[str, Any]]


class ClickhouseCluster(Cluster[SqlQuery, ClickhouseWriterOptions]):
    """
    ClickhouseCluster provides a reader, writer and Clickhouse connections that are
    shared by all storages located on the cluster.

    ClickhouseCluster is initialized with a single address (host/port/http_port),
    which is used for all read and write operations related to the cluster. This
    address can refer to either the address of the actual ClickHouse server, or a
    proxy server (e.g. for load balancing).

    However there are other operations (like some DDL operations) that must be executed
    on each individual server node, as well as each distributed table node if there
    are multiple. If we are operating a single node cluster, this is straightforward
    since there is only one server on which to run our command and no distributed table.
    If we are operating a multi node cluster we need to know the full set of shards
    and replicas on which to run our commands. This is provided by the `get_local_nodes()`
    and `get_distributed_nodes()` methods.
    """

    def __init__(
        self,
        host: str,
        port: int,
        user: str,
        password: str,
        http_port: int,
        storage_sets: Set[str],
        single_node: bool,
        # The cluster name and distributed cluster name only apply if single_node is set to False
        cluster_name: Optional[str] = None,
        distributed_cluster_name: Optional[str] = None,
    ):
        super().__init__(storage_sets)
        self.__query_node = ClickhouseNode(host, port)
        self.__user = user
        self.__password = password
        self.__http_port = http_port
        self.__single_node = single_node
        self.__cluster_name = cluster_name
        self.__distributed_cluster_name = distributed_cluster_name
        self.__reader: Optional[Reader[SqlQuery]] = None
        self.__connection_cache: MutableMapping[
            Tuple[ClickhouseNode, ClickhouseClientSettings], ClickhousePool
        ] = {}

    def __str__(self) -> str:
        return str(self.__query_node)

    def get_credentials(self) -> Tuple[str, str]:
        """
        Returns the user credentials for the Clickhouse connection
        """
        return self.__user, self.__password

    def get_query_connection(
        self, client_settings: ClickhouseClientSettings,
    ) -> ClickhousePool:
        """
        Get a connection to the query node
        """
        return self.get_node_connection(client_settings, self.__query_node)

    def get_node_connection(
        self, client_settings: ClickhouseClientSettings, node: ClickhouseNode,
    ) -> ClickhousePool:
        """
        Get a Clickhouse connection using the client settings provided. Reuse any
        connection to the same node with the same settings otherwise establish a new
        connection.
        """

        settings, timeout = client_settings.value
        cache_key = (node, client_settings)
        if cache_key not in self.__connection_cache:
            self.__connection_cache[cache_key] = ClickhousePool(
                node.host_name,
                node.port,
                self.__user,
                self.__password,
                client_settings=settings,
                send_receive_timeout=timeout,
            )

        return self.__connection_cache[cache_key]

    def get_reader(self) -> Reader[SqlQuery]:
        if not self.__reader:
            self.__reader = NativeDriverReader(
                self.get_query_connection(ClickhouseClientSettings.QUERY)
            )
        return self.__reader

    def get_writer(
        self,
        table_name: str,
        encoder: Callable[[WriterTableRow], bytes],
        options: ClickhouseWriterOptions,
        chunk_size: Optional[int],
    ) -> BatchWriter:
        return HTTPBatchWriter(
            table_name,
            self.__query_node.host_name,
            self.__http_port,
            self.__user,
            self.__password,
            encoder,
            options,
            chunk_size,
        )

    def is_single_node(self) -> bool:
        """
        This will be used to determine:
        - which migrations will be run (either just local or local and distributed tables)
        - Differences in the query - such as whether the _local or _dist table is picked
        """
        return self.__single_node

    def get_local_nodes(self) -> Sequence[ClickhouseNode]:
        if self.__single_node:
            return [self.__query_node]
        return self.__get_cluster_nodes(self.__cluster_name)

    def get_distributed_nodes(self) -> Sequence[ClickhouseNode]:
        if self.__single_node:
            return []
        return self.__get_cluster_nodes(self.__distributed_cluster_name)

    def __get_cluster_nodes(
        self, cluster_name: Optional[str]
    ) -> Sequence[ClickhouseNode]:
        return [
            ClickhouseNode(*host)
            for host in self.get_query_connection(
                ClickhouseClientSettings.QUERY
            ).execute(
                f"select host_name, port, shard_num, replica_num from system.clusters where cluster={escape_string(cluster_name)}"
            )
        ]


CLUSTERS = [
    ClickhouseCluster(
        host=cluster["host"],
        port=cluster["port"],
        user=cluster.get("user", "default"),
        password=cluster.get("password", ""),
        http_port=cluster["http_port"],
        storage_sets=cluster["storage_sets"],
        single_node=cluster["single_node"],
        cluster_name=cluster["cluster_name"] if "cluster_name" in cluster else None,
        distributed_cluster_name=cluster["distributed_cluster_name"]
        if "distributed_cluster_name" in cluster
        else None,
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
