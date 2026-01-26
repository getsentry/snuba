from __future__ import annotations

from typing import MutableMapping

from sql_metadata import Parser, QueryType  # type: ignore

from snuba import settings
from snuba.clickhouse.native import ClickhousePool
from snuba.clusters.cluster import ClickhouseClientSettings, ClickhouseCluster
from snuba.datasets.storage import ReadableTableStorage
from snuba.datasets.storages.factory import get_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.utils.serializable_exception import SerializableException


class InvalidNodeError(SerializableException):
    pass


class InvalidCustomQuery(SerializableException):
    pass


class InvalidStorageError(SerializableException):
    pass


def is_valid_node(host: str, port: int, cluster: ClickhouseCluster, storage_name: str) -> bool:
    nodes = [
        cluster.get_query_node(),
    ]
    try:
        nodes.extend([*cluster.get_local_nodes(), *cluster.get_distributed_nodes()])
    except Exception as e:
        raise InvalidNodeError(
            f"Error getting nodes for storage {storage_name}",
            extra_data={
                "error": str(e),
                "host": host,
                "port": port,
                "nodes": ",".join([node.host_name for node in nodes]),
            },
        )

    return any(node.host_name == host and node.port == port for node in nodes)


def _get_storage(storage_name: str) -> ReadableTableStorage:
    storage_key = None
    try:
        storage_key = StorageKey(storage_name)
    except ValueError:
        raise InvalidStorageError(
            f"storage {storage_name} is not a valid storage name",
            extra_data={"storage_name": storage_name},
        )
    return get_storage(storage_key)


def _validate_node(
    clickhouse_host: str,
    clickhouse_port: int,
    cluster: ClickhouseCluster,
    storage_name: str,
) -> None:
    if not is_valid_node(clickhouse_host, clickhouse_port, cluster, storage_name):
        raise InvalidNodeError(
            f"host {clickhouse_host} and port {clickhouse_port} are not valid",
            extra_data={
                "host": clickhouse_host,
                "port": clickhouse_port,
                "query_host": cluster.get_query_node().host_name,
                "query_port": cluster.get_query_node().port,
            },
        )


NODE_CONNECTIONS: MutableMapping[str, ClickhousePool] = {}


def get_ro_node_connection(
    clickhouse_host: str,
    clickhouse_port: int,
    storage_name: str,
    client_settings: ClickhouseClientSettings,
) -> ClickhousePool:
    storage = _get_storage(storage_name)

    key = f"{storage.get_storage_key()}-{clickhouse_host}"
    if key in NODE_CONNECTIONS:
        return NODE_CONNECTIONS[key]

    cluster = storage.get_cluster()
    database = cluster.get_database()
    _validate_node(clickhouse_host, clickhouse_port, cluster, storage_name)

    assert client_settings in {
        ClickhouseClientSettings.QUERY,
        ClickhouseClientSettings.QUERYLOG,
        ClickhouseClientSettings.TRACING,
        ClickhouseClientSettings.CARDINALITY_ANALYZER,
    }, (
        "admin can only use QUERY, QUERYLOG, TRACING or CARDINALITY_ANALYZER "
        "ClickhouseClientSettings"
    )

    if (
        client_settings == ClickhouseClientSettings.QUERY
        or client_settings == ClickhouseClientSettings.QUERYLOG
    ):
        username = settings.CLICKHOUSE_READONLY_USER
        password = settings.CLICKHOUSE_READONLY_PASSWORD
    else:
        username = settings.CLICKHOUSE_TRACE_USER
        password = settings.CLICKHOUSE_TRACE_PASSWORD

    connection = ClickhousePool(
        clickhouse_host,
        clickhouse_port,
        username,
        password,
        database,
        max_pool_size=2,
        client_settings=client_settings.value.settings,
    )
    NODE_CONNECTIONS[key] = connection
    return connection


CLUSTER_CONNECTIONS: MutableMapping[str, ClickhousePool] = {}


def get_ro_query_node_connection(
    storage_name: str, client_settings: ClickhouseClientSettings
) -> ClickhousePool:
    if storage_name in CLUSTER_CONNECTIONS:
        return CLUSTER_CONNECTIONS[storage_name]

    storage = _get_storage(storage_name)
    cluster = storage.get_cluster()
    connection_id = cluster.get_connection_id()
    connection = get_ro_node_connection(
        connection_id.hostname, connection_id.tcp_port, storage_name, client_settings
    )

    CLUSTER_CONNECTIONS[storage_name] = connection
    return connection


def get_sudo_node_connection(
    clickhouse_host: str,
    clickhouse_port: int,
    storage_name: str,
    client_settings: ClickhouseClientSettings,
) -> ClickhousePool:
    storage = _get_storage(storage_name)

    key = f"{storage.get_storage_key()}-{clickhouse_host}-sudo"
    if key in NODE_CONNECTIONS:
        return NODE_CONNECTIONS[key]

    cluster = storage.get_cluster()
    database = cluster.get_database()
    _validate_node(clickhouse_host, clickhouse_port, cluster, storage_name)

    (clickhouse_user, clickhouse_password) = storage.get_cluster().get_credentials()
    connection = ClickhousePool(
        clickhouse_host,
        clickhouse_port,
        clickhouse_user,
        clickhouse_password,
        database,
        max_pool_size=2,
        client_settings=client_settings.value.settings,
    )
    NODE_CONNECTIONS[key] = connection
    return connection


def get_clusterless_node_connection(
    clickhouse_host: str,
    clickhouse_port: int,
    storage_name: str,
    client_settings: ClickhouseClientSettings,
) -> ClickhousePool:
    storage = _get_storage(storage_name)
    cluster = storage.get_cluster()
    database = cluster.get_database()

    key = f"{storage.get_storage_key()}-{clickhouse_host}-clusterless-{database}"
    if key in NODE_CONNECTIONS:
        return NODE_CONNECTIONS[key]

    (clickhouse_user, clickhouse_password) = storage.get_cluster().get_credentials()
    connection = ClickhousePool(
        clickhouse_host,
        clickhouse_port,
        clickhouse_user,
        clickhouse_password,
        database,
        max_pool_size=2,
        client_settings=client_settings.value.settings,
    )
    NODE_CONNECTIONS[key] = connection
    return connection


def validate_ro_query(sql_query: str, allowed_tables: set[str] | None = None) -> None:
    """
    Simple validation to ensure query only attempts read queries.

    If allowed_tables is provided, ensures the 'from' clause contains
    an allowed table. All tables are allowed otherwise.

    Raises InvalidCustomQuery if query is invalid or not allowed.
    """
    lowered = sql_query.lower()
    disallowed_keywords = ["insert", ";"]

    for kw in disallowed_keywords:
        if kw in lowered:
            raise InvalidCustomQuery(f"{kw} is not allowed in the query")

    parsed = Parser(lowered)

    if parsed.query_type != QueryType.SELECT:
        raise InvalidCustomQuery("Only SELECT queries are allowed")

    # This parser doesn't handle ARRAY JOIN clauses correctly, so do some
    # massaging to get around that. What ends up happening is that the columns
    # in the ARRAY JOIN are treated as table aliases, so end up in this dictionary
    # as well as in the tables list. E.g. FROM x ARRAY JOIN y AS z becomes
    # tables_aliases = {'ARRAY': x, 'z': y} and tables = ['x', 'y'].
    # Confusingly it will also sometimes lower case ARRAY, so check for both.
    tables_set = set(parsed.tables)
    array_join = None
    array_join_keys = ["ARRAY", "array", "LEFT", "left"]
    for ak in array_join_keys:
        if ak in parsed.tables_aliases:
            array_join = ak
            break

    if array_join:
        for v in parsed.tables_aliases.values():
            tables_set.discard(v)  # Remove the columns

        tables_set.add(parsed.tables_aliases[array_join])  # Add the table back

    if allowed_tables and not tables_set.issubset(allowed_tables):
        raise InvalidCustomQuery(
            f"Invalid FROM clause, only the following tables are allowed: {allowed_tables}"
        )


class PreDefinedQuery:
    sql: str

    @classmethod
    def to_json(cls) -> dict[str, str]:
        return {
            "sql": cls.sql,
            "description": cls.__doc__ or "",
            "name": cls.__name__,
        }
