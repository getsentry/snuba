from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from threading import Lock
from typing import (
    Any,
    Dict,
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

import structlog

from snuba import settings
from snuba.clickhouse.escaping import escape_string
from snuba.clickhouse.http import HTTPBatchWriter, InsertStatement, JSONRow
from snuba.clickhouse.native import ClickhousePool, NativeDriverReader
from snuba.clusters.storage_sets import (
    DEV_STORAGE_SETS,
    StorageSetKey,
    register_storage_set_key,
)
from snuba.reader import Reader
from snuba.utils.metrics import MetricsBackend
from snuba.utils.serializable_exception import SerializableException
from snuba.writer import BatchWriter

logger = structlog.get_logger().bind(module=__name__)


class ClickhouseClientSettingsType(NamedTuple):
    settings: Mapping[str, Any]
    timeout: Optional[int]


class ConnectionId(NamedTuple):
    hostname: str
    tcp_port: int
    http_port: int
    database_name: str


class ClickhouseClientSettings(Enum):
    CLEANUP = ClickhouseClientSettingsType({}, None)
    INSERT = ClickhouseClientSettingsType({}, None)
    MIGRATE = ClickhouseClientSettingsType(
        {
            "load_balancing": "in_order",
            "replication_alter_partitions_sync": 2,
            "mutations_sync": 2,
            "database_atomic_wait_for_drop_and_detach_synchronously": 1,
        },
        10000,
    )
    OPTIMIZE = ClickhouseClientSettingsType({}, settings.OPTIMIZE_QUERY_TIMEOUT)
    QUERY = ClickhouseClientSettingsType({"readonly": 1}, None)
    QUERYLOG = ClickhouseClientSettingsType({}, None)
    TRACING = ClickhouseClientSettingsType({"readonly": 2}, None)
    REPLACE = (
        ClickhouseClientSettingsType(
            {
                # Replacing existing rows requires reconstructing the entire tuple
                # for each event (via a SELECT), which is a Hard Thing (TM) for
                # columnstores to do. With the default settings it's common for
                # ClickHouse to go over the default max_memory_usage of 10GB per
                # query. Lowering the max_block_size reduces memory usage, and
                # increasing the max_memory_usage gives the query more breathing
                # room.
                "max_block_size": settings.REPLACER_MAX_BLOCK_SIZE,
                "max_memory_usage": settings.REPLACER_MAX_MEMORY_USAGE,
                # Don't use up production cache for the count() queries.
                "use_uncompressed_cache": 0,
            },
            None,
        ),
    )
    CARDINALITY_ANALYZER = ClickhouseClientSettingsType(
        {
            # Allow reading data and changing settings.
            "readonly": 2,
            # Allow more threads for faster processing since cardinality queries
            # need more resources.
            "max_threads": 10,
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


class ClickhouseNodeType(Enum):
    LOCAL = "local"
    DIST = "dist"


TWriterOptions = TypeVar("TWriterOptions")


class Cluster(ABC, Generic[TWriterOptions]):
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
        # register the cluster's storage sets
        for storage_set in storage_sets:
            register_storage_set_key(storage_set)

    def get_storage_set_keys(self) -> Set[StorageSetKey]:
        return {StorageSetKey(storage_set) for storage_set in self.__storage_sets}

    @abstractmethod
    def get_reader(self) -> Reader:
        raise NotImplementedError

    @abstractmethod
    def get_batch_writer(
        self,
        metrics: MetricsBackend,
        insert_statement: InsertStatement,
        encoding: Optional[str],
        options: TWriterOptions,
        chunk_size: Optional[int],
        buffer_size: int,
    ) -> BatchWriter[JSONRow]:
        raise NotImplementedError


ClickhouseWriterOptions = Optional[Mapping[str, Any]]


CacheKey = Tuple[ClickhouseNode, ClickhouseClientSettings, str, str, str]


class ConnectionCache:
    def __init__(self) -> None:
        self.__cache: MutableMapping[CacheKey, ClickhousePool] = {}
        self.__lock = Lock()

    def get_node_connection(
        self,
        client_settings: ClickhouseClientSettings,
        node: ClickhouseNode,
        user: str,
        password: str,
        database: str,
    ) -> ClickhousePool:
        with self.__lock:
            settings, timeout = client_settings.value
            cache_key = (node, client_settings, user, password, database)
            if cache_key not in self.__cache:
                self.__cache[cache_key] = ClickhousePool(
                    node.host_name,
                    node.port,
                    user,
                    password,
                    database,
                    client_settings=settings,
                    send_receive_timeout=timeout,
                )

            return self.__cache[cache_key]


connection_cache = ConnectionCache()


class ClickhouseCluster(Cluster[ClickhouseWriterOptions]):
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
        database: str,
        http_port: int,
        storage_sets: Set[str],
        single_node: bool,
        # The cluster name and distributed cluster name only apply if single_node is set to False
        cluster_name: Optional[str] = None,
        distributed_cluster_name: Optional[str] = None,
        cache_partition_id: Optional[str] = None,
        query_settings_prefix: Optional[str] = None,
    ):
        super().__init__(storage_sets)
        self.__host = host
        self.__port = port
        self.__query_node = ClickhouseNode(host, port)
        self.__user = user
        self.__password = password
        self.__database = database
        self.__http_port = http_port
        self.__single_node = single_node
        self.__cluster_name = cluster_name
        self.__distributed_cluster_name = distributed_cluster_name
        self.__reader: Optional[Reader] = None
        self.__connection_cache = connection_cache
        self.__cache_partition_id = cache_partition_id
        self.__query_settings_prefix = query_settings_prefix

    def __str__(self) -> str:
        return str(self.__query_node)

    def get_credentials(self) -> Tuple[str, str]:
        """
        Returns the user credentials for the Clickhouse connection
        """
        return self.__user, self.__password

    def get_query_connection(
        self,
        client_settings: ClickhouseClientSettings,
    ) -> ClickhousePool:
        """
        Get a connection to the query node
        """
        return self.get_node_connection(client_settings, self.__query_node)

    def get_node_connection(
        self,
        client_settings: ClickhouseClientSettings,
        node: ClickhouseNode,
    ) -> ClickhousePool:
        """
        Get a Clickhouse connection using the client settings provided. Reuse any
        connection to the same node with the same settings otherwise establish a new
        connection.
        """

        return self.__connection_cache.get_node_connection(
            client_settings,
            node,
            self.__user,
            self.__password,
            self.__database,
        )

    def get_reader(self) -> Reader:
        if not self.__reader:
            self.__reader = NativeDriverReader(
                cache_partition_id=self.__cache_partition_id,
                client=self.get_query_connection(ClickhouseClientSettings.QUERY),
                query_settings_prefix=self.__query_settings_prefix,
            )
        return self.__reader

    def get_batch_writer(
        self,
        metrics: MetricsBackend,
        insert_statement: InsertStatement,
        encoding: Optional[str],
        options: ClickhouseWriterOptions,
        chunk_size: Optional[int],
        buffer_size: int,
    ) -> BatchWriter[JSONRow]:
        return HTTPBatchWriter(
            host=self.__query_node.host_name,
            port=self.__http_port,
            user=self.__user,
            password=self.__password,
            metrics=metrics,
            statement=insert_statement.with_database(self.__database),
            encoding=encoding,
            options=options,
            chunk_size=chunk_size,
            buffer_size=buffer_size,
        )

    def is_single_node(self) -> bool:
        """
        This will be used to determine:
        - which migrations will be run (either just local or local and distributed tables)
        - Differences in the query - such as whether the _local or _dist table is picked
        """
        return self.__single_node

    def get_clickhouse_cluster_name(self) -> Optional[str]:
        return self.__cluster_name

    def get_database(self) -> str:
        return self.__database

    def get_query_node(self) -> ClickhouseNode:
        return self.__query_node

    def get_local_nodes(self) -> Sequence[ClickhouseNode]:
        if self.__single_node:
            return [self.__query_node]

        assert self.__cluster_name is not None, "cluster_name must be set"
        return self.__get_cluster_nodes(self.__cluster_name)

    def get_distributed_nodes(self) -> Sequence[ClickhouseNode]:
        if self.__single_node:
            return []
        if self.__distributed_cluster_name is None:
            logger.warning(
                "distributed_cluster_name is not set, but is_single_node is False."
                "This is likely a configuration error. Returning empty list."
            )
            return []
        assert (
            self.__distributed_cluster_name is not None
        ), "distributed_cluster_name must be set"
        return self.__get_cluster_nodes(self.__distributed_cluster_name)

    def get_connection_id(self) -> ConnectionId:
        return ConnectionId(
            hostname=self.__query_node.host_name,
            tcp_port=self.__query_node.port,
            http_port=self.__http_port,
            database_name=self.__database,
        )

    def __get_cluster_nodes(self, cluster_name: str) -> Sequence[ClickhouseNode]:
        return [
            ClickhouseNode(*host)
            for host in self.get_query_connection(ClickhouseClientSettings.QUERY)
            .execute(
                f"select host_name, port, shard_num, replica_num from system.clusters where cluster={escape_string(cluster_name)}"
            )
            .results
        ]

    def get_host(self) -> str:
        return self.__host

    def get_port(self) -> int:
        return self.__port

    def get_http_port(self) -> int:
        return self.__http_port


CLUSTERS = [
    ClickhouseCluster(
        host=cluster["host"],
        port=cluster["port"],
        user=cluster.get("user", "default"),
        password=cluster.get("password", ""),
        database=cluster.get("database", "default"),
        http_port=cluster["http_port"],
        storage_sets=cluster["storage_sets"],
        single_node=cluster["single_node"],
        cluster_name=cluster["cluster_name"] if "cluster_name" in cluster else None,
        distributed_cluster_name=cluster["distributed_cluster_name"]
        if "distributed_cluster_name" in cluster
        else None,
        cache_partition_id=cluster.get("cache_partition_id"),
        query_settings_prefix=cluster.get("query_settings_prefix"),
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

_STORAGE_SET_CLUSTER_MAP: Dict[StorageSetKey, ClickhouseCluster] = {
    storage_set: cluster
    for cluster in CLUSTERS
    for storage_set in cluster.get_storage_set_keys()
}


def _get_storage_set_cluster_map() -> Dict[StorageSetKey, ClickhouseCluster]:
    return _STORAGE_SET_CLUSTER_MAP


def _build_sliced_cluster(cluster: Mapping[str, Any]) -> ClickhouseCluster:
    return ClickhouseCluster(
        host=cluster["host"],
        port=cluster["port"],
        user=cluster.get("user", "default"),
        password=cluster.get("password", ""),
        database=cluster.get("database", "default"),
        http_port=cluster["http_port"],
        storage_sets={
            storage_tuple[0] for storage_tuple in cluster["storage_set_slices"]
        },
        single_node=cluster["single_node"],
        cluster_name=cluster["cluster_name"] if "cluster_name" in cluster else None,
        distributed_cluster_name=cluster["distributed_cluster_name"]
        if "distributed_cluster_name" in cluster
        else None,
        cache_partition_id=cluster.get("cache_partition_id"),
        query_settings_prefix=cluster.get("query_settings_prefix"),
    )


_SLICED_STORAGE_SET_CLUSTER_MAP: Dict[Tuple[StorageSetKey, int], ClickhouseCluster] = {}


def _get_sliced_storage_set_cluster_map() -> Dict[
    Tuple[StorageSetKey, int], ClickhouseCluster
]:
    if len(_SLICED_STORAGE_SET_CLUSTER_MAP) == 0:
        for cluster in settings.SLICED_CLUSTERS:
            for storage_set_tuple in cluster["storage_set_slices"]:
                _SLICED_STORAGE_SET_CLUSTER_MAP[
                    (StorageSetKey(storage_set_tuple[0]), storage_set_tuple[1])
                ] = _build_sliced_cluster(cluster)

    return _SLICED_STORAGE_SET_CLUSTER_MAP


class UndefinedClickhouseCluster(SerializableException):
    pass


def get_cluster(
    storage_set_key: StorageSetKey, slice_id: Optional[int] = None
) -> ClickhouseCluster:
    """Return a clickhouse cluster for a storage set key.

    If passing in a sliced storage set, a slice_id must be specified.
    This ID will be used to return the matching cluster in SLICED_CLUSTERS.
    If passing in an non-sliced storage set, a slice_id should not be
    specified. The StorageSetKey will be used to return the matching
    cluster in CLUSTERS.

    If the storage set key is not defined either in CLUSTERS or in
    SLICED_CLUSTERS, then an UndefinedClickhouseCluster Exception
    will be raised.
    """
    assert (
        storage_set_key not in DEV_STORAGE_SETS or settings.ENABLE_DEV_FEATURES
    ), f"Storage set {storage_set_key} is disabled"

    if slice_id is not None:
        part_storage_set_cluster_map = _get_sliced_storage_set_cluster_map()
        res = part_storage_set_cluster_map.get((storage_set_key, slice_id), None)
        if res is None:
            raise UndefinedClickhouseCluster(
                f"{(storage_set_key, slice_id)} is not defined in the SLICED_CLUSTERS setting for this environment",
                storage_set_key_not_defined=storage_set_key.value,
                slice_id=slice_id,
            )

    else:
        storage_set_cluster_map = _get_storage_set_cluster_map()
        res = storage_set_cluster_map.get(storage_set_key, None)
        if res is None:
            raise UndefinedClickhouseCluster(
                f"{storage_set_key} is not defined in the CLUSTERS setting for this environment",
                storage_set_key_not_defined=storage_set_key.value,
            )
    return res
