import os
from abc import ABC, abstractmethod
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from enum import Enum
from functools import cache
from typing import (
    Any,
    Generic,
    NamedTuple,
    TypeVar,
)

import structlog

from snuba import settings
from snuba.clickhouse.connect import ClickhouseConnectPool
from snuba.clickhouse.http import HTTPBatchWriter, InsertStatement, JSONRow
from snuba.clickhouse.pool import ClickhousePool
from snuba.clickhouse.reader import ClickhouseReader
from snuba.clusters.storage_sets import (
    DEV_STORAGE_SETS,
    StorageSetKey,
    register_storage_set_key,
)
from snuba.reader import Reader
from snuba.state.sentry_options import get_option
from snuba.utils.metrics import MetricsBackend
from snuba.utils.serializable_exception import SerializableException
from snuba.writer import BatchWriter

logger = structlog.get_logger().bind(module=__name__)

# Well-known default ClickHouse HTTP port, used by by-host helpers (e.g. CLI
# tools) that only know a node's address and have no cluster config to read an
# port from.
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
    port: int
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
    QUERY = ClickhouseClientSettingsType({}, _DEFAULT_USER_FACING_TIMEOUT)
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
            # columnstores to do. Keep conservative defaults on the client
            # profile (1 thread, small blocks, 10GB). Per-environment tuning
            # is via sentry-options and applied per query in the replacer
            # (see get_replace_query_settings) so option flips do not require
            # rebuilding the cached REPLACE pool.
            "max_block_size": settings.REPLACER_MAX_BLOCK_SIZE,
            "max_threads": settings.REPLACER_MAX_THREADS,
            "max_memory_usage": settings.REPLACER_MAX_MEMORY_USAGE,
            # Don't use up production cache for replacement SELECT ... FINAL.
            "use_uncompressed_cache": 0,
            # Same FINAL setting user reads already apply. errors partitions are
            # (retention_days, toMonday(timestamp)), so cross-partition merges
            # are unnecessary and expensive under FINAL.
            "do_not_merge_across_partitions_select_final": 1,
            # Skip indexes (e.g. minmax_group_id) used to be ignored under FINAL.
            # Keep them on so group_id filters can still prune granules.
            "use_skip_indexes_if_final": 1,
            # Delete/merge/unmerge leave predicates in WHERE. FINAL ignores
            # optimize_move_to_prewhere unless this is also on (default off).
            "optimize_move_to_prewhere": 1,
            "optimize_move_to_prewhere_if_final": 1,
            # clickhouse-connect caps its own progress interval at 120s when the
            # client timeout is large. Pin a 15s header so a quiet FINAL still
            # writes bytes before http_send_timeout / Envoy idle (30s default).
            "send_progress_in_http_headers": 1,
            "http_headers_progress_interval_ms": 15000,
            # Server-side kill switch. Client timeout is intentionally a bit
            # higher (REPLACER_CLIENT_TIMEOUT) so CH can surface this error
            # before the HTTP read times out.
            "max_execution_time": settings.REPLACER_QUERY_TIMEOUT,
        },
        # seconds; clickhouse-connect maps this to urllib3 Timeout(read=...)
        settings.REPLACER_CLIENT_TIMEOUT,
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


def get_replace_query_settings() -> dict[str, int]:
    """Per-query ClickHouse settings for REPLACE INSERT ... SELECT FINAL.

    Read from sentry-options so each environment can raise threads/block size/
    memory on large errors replicas without shipping a code change. Defaults
    stay conservative (1 thread, 512-row blocks, 10GB). Applied at execute
    time rather than on the cached REPLACE client profile so option flips
    take effect without rebuilding pools.
    """
    return {
        "max_threads": get_option("replacer_max_threads", settings.REPLACER_MAX_THREADS),
        "max_block_size": get_option(
            "replacer_max_block_size", settings.REPLACER_MAX_BLOCK_SIZE
        ),
        "max_memory_usage": get_option(
            "replacer_max_memory_usage", settings.REPLACER_MAX_MEMORY_USAGE
        ),
    }


@dataclass(frozen=True)
class ClickhouseNode:
    host_name: str
    port: int
    shard: int | None = None
    replica: int | None = None

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


@cache
def _build_pool_cached(
    client_settings: ClickhouseClientSettings,
    host: str,
    port: int,
    user: str,
    password: str,
    database: str | None,
    secure: bool,
    ca_certs: str | None,
    verify: bool | None,
) -> ClickhousePool:
    """Return the process-local pool for this exact endpoint and configuration."""
    client_settings_dict, timeout = client_settings.value
    return ClickhouseConnectPool(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
        client_settings=client_settings_dict,
        send_receive_timeout=timeout,
        secure=secure,
        ca_certs=ca_certs,
        verify=verify,
    )


def _clear_pool_cache_after_fork() -> None:
    """Drop inherited pool objects so children never reuse parent locks/clients."""
    _build_pool_cached.cache_clear()


os.register_at_fork(after_in_child=_clear_pool_cache_after_fork)


def build_pool(
    client_settings: ClickhouseClientSettings,
    node: ClickhouseNode,
    user: str,
    password: str,
    database: str | None,
    secure: bool = False,
    ca_certs: str | None = None,
    verify: bool | None = None,
) -> ClickhousePool:
    """Return a cached client pool scoped to one cluster endpoint and configuration.

    Client construction probes ClickHouse for server settings and timezone.
    Host and port are explicit cache-key fields so clients are never shared
    across cluster endpoints. Credentials, database, TLS, and client settings
    also remain part of the key.
    """
    return _build_pool_cached(
        client_settings,
        node.host_name,
        node.port,
        user,
        password,
        database,
        secure,
        ca_certs,
        verify,
    )


_DEFAULT_MAX_CONNECTIONS = 1


class ClickhouseCluster(Cluster[ClickhouseWriterOptions]):
    """
    ClickhouseCluster provides a reader, writer and Clickhouse connections that are
    shared by all storages located on the cluster.

    ClickhouseCluster is initialized with a single address (host/port),
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
        self.__query_node = ClickhouseNode(host, port)
        self.__user = user
        self.__password = password
        self.__database = database
        self.__secure = secure
        self.__ca_certs = ca_certs
        self.__verify = verify
        self.__single_node = single_node
        self.__cluster_name = cluster_name
        self.__distributed_cluster_name = distributed_cluster_name
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
        Build a Clickhouse connection using the client settings provided.
        """
        return build_pool(
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
        Return a reader for the query node. ClickhouseReader wraps the
        clickhouse-connect pool from get_query_connection.
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
            port=self.__port,
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
            port=self.__port,
            database_name=self.__database,
        )

    def __get_cluster_nodes(self, cluster_name: str) -> Sequence[ClickhouseNode]:
        # system.clusters reports the TCP port; discard it. Replicas serve HTTP
        # on 8123. Envoy only fronts the query endpoint.
        return [
            ClickhouseNode(
                host_name=host[0],
                port=DEFAULT_CLICKHOUSE_HTTP_PORT,
                shard=host[2],
                replica=host[3],
            )
            for host in self.get_query_connection(ClickhouseClientSettings.INTERNAL)
            .execute(
                "select host_name, port, shard_num, replica_num from system.clusters where cluster=%(cluster_name)s",
                {"cluster_name": cluster_name},
            )
            .results
        ]

    def get_host(self) -> str:
        return self.__host

    def get_port(self) -> int:
        return self.__port

    def get_secure(self) -> bool:
        return self.__secure

    def get_ca_certs(self) -> str | None:
        return self.__ca_certs

    def get_verify(self) -> bool | None:
        return self.__verify


CLUSTERS = [
    ClickhouseCluster(
        host=cluster["host"],
        # Prefer http_port when present so older dual-port configs still dial HTTP.
        port=cluster.get("http_port", cluster["port"]),
        user=cluster.get("user", "default"),
        password=cluster.get("password", ""),
        database=cluster.get("database", "default"),
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
        # Prefer http_port when present so older dual-port configs still dial HTTP.
        port=cluster.get("http_port", cluster["port"]),
        user=cluster.get("user", "default"),
        password=cluster.get("password", ""),
        database=cluster.get("database", "default"),
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
