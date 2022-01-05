from typing import MutableMapping

from snuba import settings
from snuba.clickhouse.native import ClickhousePool
from snuba.clusters.cluster import ClickhouseClientSettings, ClickhouseCluster
from snuba.datasets.storages import StorageKey
from snuba.datasets.storages.factory import get_storage
from snuba.utils.serializable_exception import SerializableException


class InvalidNodeError(SerializableException):
    pass


class InvalidCustomQuery(SerializableException):
    pass


class InvalidStorageError(SerializableException):
    pass


def is_valid_node(host: str, port: int, cluster: ClickhouseCluster) -> bool:
    nodes = cluster.get_local_nodes()
    return any(node.host_name == host and node.port == port for node in nodes)


CONNECTIONS: MutableMapping[StorageKey, ClickhousePool] = {}


def get_clickhouse_connection(
    clickhouse_host: str, clickhouse_port: int, storage_name: str
) -> ClickhousePool:
    storage_key = None
    try:
        storage_key = StorageKey(storage_name)
    except ValueError:
        raise InvalidStorageError(
            f"storage {storage_name} is not a valid storage name",
            extra_data={"storage_name": storage_name},
        )

    if storage_key in CONNECTIONS:
        return CONNECTIONS[storage_key]

    storage = get_storage(storage_key)
    cluster = storage.get_cluster()

    if not is_valid_node(clickhouse_host, clickhouse_port, cluster):
        raise InvalidNodeError(
            f"host {clickhouse_host} and port {clickhouse_port} are not valid",
            extra_data={"host": clickhouse_host, "port": clickhouse_port},
        )

    database = cluster.get_database()
    connection = ClickhousePool(
        clickhouse_host,
        clickhouse_port,
        settings.CLICKHOUSE_READONLY_USER,
        settings.CLICKHOUSE_READONLY_PASSWORD,
        database,
        # force read-only
        client_settings=ClickhouseClientSettings.QUERY.value.settings,
    )
    CONNECTIONS[storage_key] = connection
    return connection
