from abc import ABC, abstractmethod
from collections.abc import Mapping, MutableMapping, Sequence
from dataclasses import dataclass
from enum import Enum
from threading import Lock
from typing import (
    Any,
    Generic,
    NamedTuple,
    TypeVar,
)

import structlog

from snuba import settings, state
from snuba.clickhouse.http import HTTPBatchWriter, InsertStatement, JSONRow
from snuba.clickhouse.native import (
    ClickhouseNativePool,
    ClickhousePool,
    ClickhouseReader,
)
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

# Well-known default ClickHouse HTTP port, used by by-host helpers (e.g. CLI
# tools) that only know a node's native address and have no cluster config to
# read an http_port from.
DEFAULT_CLICKHOUSE_HTTP_PORT = 8123
# User-facing read queries get a 25s timeout, leaving headroom under a ~30s
# frontend request budget to still return a response. Migrations, DDL and
# other long-running operations keep their own (default or longer) timeouts
# above/below.
_DEFAULT_USER_FACING_TIMEOUT = 25


class ClickhouseClientSettingsType(NamedTuple):
    settings: Mapping[str, Any]
    timeout: int | None


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
            "alter_sync": 2,  # Wait for ON CLUSTER DDL on all replicas
            "database_atomic_wait_for_drop_and_detach_synchronously": 1,
            "distributed_ddl_task_timeout": 300,  # 5 minute ON CLUSTER DDL timeout
        },
        # 5 minute timeout to allow ON CLUSTER DDL operations to complete
        # across all replicas. This is needed because alter_sync=2 blocks
        # until all replicas confirm completion.
        300000,
    )
    DELETE = ClickhouseClientSettingsType({"mutations_sync": 1}, None)
    OPTIMIZE = ClickhouseClientSettingsType({}, settings.OPTIMIZE_QUERY_TIMEOUT)
    QUERY = ClickhouseClientSettingsType(
        {"max_execution_time": _DEFAULT_USER_FACING_TIMEOUT}, _DEFAULT_USER_FACING_TIMEOUT
    )
    TRACING = ClickhouseClientSettingsType(
        {"readonly": 2, "max_execution_time": _DEFAULT_USER_FACING_TIMEOUT},
        _DEFAULT_USER_FACING_TIMEOUT,
    )
    # Internal/maintenance queries that are NOT user-facing reads and must not
    # inherit QUERY's 25s cap: cluster topology discovery (system.clusters),
    # storage-routing load lookups, delete-throttling system-table checks, the
    # span-export job and admin table copies. These can legitimately run long,
    # so they stay unbounded (their behavior before QUERY got a read timeout).
    INTERNAL = ClickhouseClientSettingsType({}, None)
    QUERYLOG = ClickhouseClientSettingsType({}, None)
    REPLACE = ClickhouseClientSettingsType(
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
    )
    CARDINALITY_ANALYZER = ClickhouseClientSettingsType(
        {
            # Allow reading data and changing settings.
            "readonly": 2,
            # Allow more threads for faster processing since cardinality queries
            # need more resources.
            "max_threads": 10,
            # Don't use up production cache for cardinality analyzer queries.
            "use_uncompressed_cache": 0,
            # Allow longer running queries.
            "max_execution_time": 60,
        },
        None,
    )


@dataclass(frozen=True)
class ClickhouseNode:
    host_name: str
    native_port: int
    shard: int | None = None
    replica: int | None = None
    # The node's HTTP port, used by the clickhouse-connect (HTTP) driver. It is
    # optional because nodes built outside a cluster context (e.g. in tests, or
    # the replacer's load balancer, which never open HTTP connections) do not
    # need one. Cluster-produced nodes always carry the cluster's HTTP port.
    http_port: int | None = None

    def __str__(self) -> str:
        return f"{self.host_name}:{self.native_port}"


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

    def __init__(self, storage_sets: set[str]):
        self.__storage_sets = storage_sets
        # register the cluster's storage sets
        for storage_set in storage_sets:
            register_storage_set_key(storage_set)

    def get_storage_set_keys(self) -> set[StorageSetKey]:
        return {StorageSetKey(storage_set) for storage_set in self.__storage_sets}

    @abstractmethod
    def get_reader(self) -> Reader:
        raise NotImplementedError

    @abstractmethod
    def get_batch_writer(
        self,
        metrics: MetricsBackend,
        insert_statement: InsertStatement,
        encoding: str | None,
        options: TWriterOptions,
        chunk_size: int | None,
        buffer_size: int,
    ) -> BatchWriter[JSONRow]:
        raise NotImplementedError


ClickhouseWriterOptions = Mapping[str, Any] | None


def use_clickhouse_connect_driver() -> bool:
    """
    Whether the read path should use the clickhouse-connect (HTTP) driver
    instead of the native protocol.

    Controlled by a runtime config flag (defaulting to the
    ``USE_CLICKHOUSE_CONNECT_DRIVER`` setting) so the migration can be rolled
    out and rolled back without a deploy.
    """
    default = 1 if settings.USE_CLICKHOUSE_CONNECT_DRIVER else 0
    return bool(state.get_int_config("use_clickhouse_connect_driver", default))


# The driver discriminator is part of the cache key so that the native and the
# HTTP pool for the same node can be cached side by side. The node's HTTP port
# is part of ``ClickhouseNode`` itself, so it does not need a separate key
# element.
CacheKey = tuple[
    ClickhouseNode,
    ClickhouseClientSettings,
    str,
    str,
    str,
    bool,
    str | None,
    bool | None,
    str,
]


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
        secure: bool,
        ca_certs: str | None,
        verify: bool | None,
    ) -> ClickhousePool:
        """
        Return a cached connection pool for the node, typed as the abstract
        :class:`ClickhousePool`. The driver is decided here, from the
        ``use_clickhouse_connect_driver`` runtime config: when it is enabled the
        clickhouse-connect (HTTP) pool is built (connecting on the node's
        ``http_port``), otherwise the native one (connecting on the node's
        ``native_port``). Both variants are cached side by side (the driver is
        part of the cache key).

        This is the single place pools are instantiated and the single place the
        driver is selected, so every caller — the cluster query/node connections
        as well as the admin and CLI by-host helpers — goes through it and gets
        one shared, runtime-selected pool behind the abstract
        :class:`ClickhousePool` type. Pool sizing is left to the pools themselves
        (the connect pool reads the ``clickhouse_connect_pool_size`` runtime
        config).
        """
        use_connect = use_clickhouse_connect_driver()
        with self.__lock:
            client_settings_dict, timeout = client_settings.value
            cache_key = (
                node,
                client_settings,
                user,
                password,
                database,
                secure,
                ca_certs,
                verify,
                "http" if use_connect else "native",
            )
            if cache_key not in self.__cache:
                pool: ClickhousePool
                if use_connect:
                    # Imported here so that the native code path never imports
                    # clickhouse-connect.
                    from snuba.clickhouse.connect import ClickhouseConnectPool

                    pool = ClickhouseConnectPool(
                        host=node.host_name,
                        # Fall back to the default HTTP port only for nodes that
                        # were built without one (e.g. by-host helpers that have
                        # no cluster http_port to draw on).
                        http_port=(
                            node.http_port
                            if node.http_port is not None
                            else DEFAULT_CLICKHOUSE_HTTP_PORT
                        ),
                        user=user,
                        password=password,
                        database=database,
                        client_settings=client_settings_dict,
                        send_receive_timeout=timeout,
                        secure=secure,
                        ca_certs=ca_certs,
                        verify=verify,
                    )
                else:
                    pool = ClickhouseNativePool(
                        node.host_name,
                        node.native_port,
                        user,
                        password,
                        database,
                        client_settings=client_settings_dict,
                        send_receive_timeout=timeout,
                        secure=secure,
                        ca_certs=ca_certs,
                        verify=verify,
                    )
                self.__cache[cache_key] = pool

            return self.__cache[cache_key]


connection_cache = ConnectionCache()
_DEFAULT_MAX_CONNECTIONS = 1


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
        secure: bool,
        ca_certs: str | None,
        verify: bool | None,
        storage_sets: set[str],
        single_node: bool,
        # The cluster name and distributed cluster name only apply if single_node is set to False
        cluster_name: str | None = None,
        distributed_cluster_name: str | None = None,
        cache_partition_id: str | None = None,
        query_settings_prefix: str | None = None,
        max_connections: int | None = None,
        block_connections: bool = False,
    ):
        super().__init__(storage_sets)
        self.__host = host
        self.__port = port
        self.__max_connections = max_connections or _DEFAULT_MAX_CONNECTIONS
        self.__block_connections = block_connections
        self.__query_node = ClickhouseNode(host, port, http_port=http_port)
        self.__user = user
        self.__password = password
        self.__database = database
        self.__http_port = http_port
        self.__secure = secure
        self.__ca_certs = ca_certs
        self.__verify = verify
        self.__single_node = single_node
        self.__cluster_name = cluster_name
        self.__distributed_cluster_name = distributed_cluster_name
        self.__connection_cache = connection_cache
        self.__cache_partition_id = cache_partition_id
        self.__query_settings_prefix = query_settings_prefix
        # The local node used by the deleter is static cluster topology; cache
        # it so get_deleter() does not re-run a system.clusters lookup per call.
        self.__delete_local_node: ClickhouseNode | None = None

    def __str__(self) -> str:
        return str(self.__query_node)

    def get_credentials(self) -> tuple[str, str]:
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

        The driver is selected inside ``ConnectionCache.get_node_connection``
        from the ``use_clickhouse_connect_driver`` runtime config (HTTP pool
        when enabled, native otherwise). The choice applies to every caller
        (reads, migrations, replacer, optimize, ...), not just the read path.
        """
        return self.__connection_cache.get_node_connection(
            client_settings,
            node,
            self.__user,
            self.__password,
            self.__database,
            self.__secure,
            self.__ca_certs,
            self.__verify,
        )

    def get_deleter(self) -> Reader:
        # we need the connection to the storage nodes, not the distributed
        # nodes. The node lookup is cached (it can run a system.clusters query
        # on multi-node clusters) while the connection is resolved per call so
        # the driver can still switch at runtime.
        if self.__delete_local_node is None:
            self.__delete_local_node = self.get_local_nodes()[0]
        return ClickhouseReader(
            cache_partition_id=f"{self.__cache_partition_id}_deletes",
            client=self.get_node_connection(
                ClickhouseClientSettings.DELETE, self.__delete_local_node
            ),
            query_settings_prefix=self.__query_settings_prefix,
        )

    def get_reader(self) -> Reader:
        """
        Return a reader for the query node. The driver-agnostic ClickhouseReader
        wraps whichever pool (native or HTTP) get_query_connection selects from
        the ``use_clickhouse_connect_driver`` runtime config, so the driver can
        be switched at runtime.
        """
        return ClickhouseReader(
            cache_partition_id=self.__cache_partition_id,
            client=self.get_query_connection(ClickhouseClientSettings.QUERY),
            query_settings_prefix=self.__query_settings_prefix,
        )

    def get_batch_writer(
        self,
        metrics: MetricsBackend,
        insert_statement: InsertStatement,
        encoding: str | None,
        options: ClickhouseWriterOptions,
        chunk_size: int | None,
        buffer_size: int,
    ) -> BatchWriter[JSONRow]:
        return HTTPBatchWriter(
            host=self.__query_node.host_name,
            port=self.__http_port,
            max_connections=self.__max_connections,
            block_connections=self.__block_connections,
            user=self.__user,
            password=self.__password,
            secure=self.__secure,
            ca_certs=self.__ca_certs,
            verify=self.__verify,
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

    def get_clickhouse_cluster_name(self) -> str | None:
        return self.__cluster_name

    def get_clickhouse_distributed_cluster_name(self) -> str | None:
        return self.__distributed_cluster_name

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
        assert self.__distributed_cluster_name is not None, "distributed_cluster_name must be set"
        return self.__get_cluster_nodes(self.__distributed_cluster_name)

    def get_connection_id(self) -> ConnectionId:
        return ConnectionId(
            hostname=self.__query_node.host_name,
            tcp_port=self.__query_node.native_port,
            http_port=self.__http_port,
            database_name=self.__database,
        )

    def __get_cluster_nodes(self, cluster_name: str) -> Sequence[ClickhouseNode]:
        # system.clusters only reports the native port. The cluster's configured
        # HTTP port is an Envoy intercept port that only fronts the cluster
        # endpoint (the query node); individual nodes discovered here are
        # addressed directly, bypassing Envoy, and serve HTTP on the well-known
        # default port. Stamp that on each node so the HTTP driver connects to a
        # port the node actually listens on.
        return [
            ClickhouseNode(
                host_name=host[0],
                native_port=host[1],
                shard=host[2],
                replica=host[3],
                http_port=DEFAULT_CLICKHOUSE_HTTP_PORT,
            )
            for host in self.get_query_connection(ClickhouseClientSettings.INTERNAL)
            .execute(
                "select host_name, port, shard_num, replica_num from system.clusters where cluster=%(cluster_name)s",
                {"cluster_name": cluster_name},
                retryable=True,
            )
            .results
        ]

    def get_host(self) -> str:
        return self.__host

    def get_port(self) -> int:
        return self.__port

    def get_http_port(self) -> int:
        return self.__http_port

    def get_secure(self) -> bool:
        return self.__secure


CLUSTERS = [
    ClickhouseCluster(
        host=cluster["host"],
        port=cluster["port"],
        user=cluster.get("user", "default"),
        password=cluster.get("password", ""),
        database=cluster.get("database", "default"),
        http_port=cluster["http_port"],
        secure=cluster.get("secure", False),
        ca_certs=cluster.get("ca_certs", None),
        verify=cluster.get("verify", False),
        storage_sets=cluster["storage_sets"],
        single_node=cluster["single_node"],
        cluster_name=cluster.get("cluster_name", None),
        distributed_cluster_name=(cluster.get("distributed_cluster_name", None)),
        cache_partition_id=cluster.get("cache_partition_id"),
        query_settings_prefix=cluster.get("query_settings_prefix"),
        max_connections=cluster.get("max_connections", _DEFAULT_MAX_CONNECTIONS),
    )
    for cluster in settings.CLUSTERS
]

_registered_storage_sets = [
    storage_set for cluster in CLUSTERS for storage_set in cluster.get_storage_set_keys()
]

_unique_registered_storage_sets = set(_registered_storage_sets)

assert len(_registered_storage_sets) == len(_unique_registered_storage_sets), (
    "Storage set registered to more than one cluster"
)

_STORAGE_SET_CLUSTER_MAP: dict[StorageSetKey, ClickhouseCluster] = {
    storage_set: cluster for cluster in CLUSTERS for storage_set in cluster.get_storage_set_keys()
}


def _get_storage_set_cluster_map() -> dict[StorageSetKey, ClickhouseCluster]:
    return _STORAGE_SET_CLUSTER_MAP


def _build_sliced_cluster(cluster: Mapping[str, Any]) -> ClickhouseCluster:
    return ClickhouseCluster(
        host=cluster["host"],
        port=cluster["port"],
        user=cluster.get("user", "default"),
        password=cluster.get("password", ""),
        database=cluster.get("database", "default"),
        http_port=cluster["http_port"],
        secure=cluster.get("secure", False),
        ca_certs=cluster.get("ca_certs", None),
        verify=cluster.get("verify", False),
        storage_sets={storage_tuple[0] for storage_tuple in cluster["storage_set_slices"]},
        single_node=cluster["single_node"],
        cluster_name=cluster.get("cluster_name", None),
        distributed_cluster_name=(cluster.get("distributed_cluster_name", None)),
        cache_partition_id=cluster.get("cache_partition_id"),
        query_settings_prefix=cluster.get("query_settings_prefix"),
    )


_SLICED_STORAGE_SET_CLUSTER_MAP: dict[tuple[StorageSetKey, int], ClickhouseCluster] = {}


def _get_sliced_storage_set_cluster_map() -> dict[tuple[StorageSetKey, int], ClickhouseCluster]:
    if len(_SLICED_STORAGE_SET_CLUSTER_MAP) == 0:
        for cluster in settings.SLICED_CLUSTERS:
            for storage_set_tuple in cluster["storage_set_slices"]:
                _SLICED_STORAGE_SET_CLUSTER_MAP[
                    (StorageSetKey(storage_set_tuple[0]), storage_set_tuple[1])
                ] = _build_sliced_cluster(cluster)

    return _SLICED_STORAGE_SET_CLUSTER_MAP


class UndefinedClickhouseCluster(SerializableException):
    pass


def get_cluster(storage_set_key: StorageSetKey, slice_id: int | None = None) -> ClickhouseCluster:
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
    assert storage_set_key not in DEV_STORAGE_SETS or settings.ENABLE_DEV_FEATURES, (
        f"Storage set {storage_set_key} is disabled"
    )

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
