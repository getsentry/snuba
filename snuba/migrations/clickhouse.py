CLICKHOUSE_SERVER_MIN_VERSION = "20.3.9.70"
from typing import MutableMapping

from snuba import settings
from snuba.clickhouse.native import ClickhousePool
from snuba.clusters.cluster import ClickhouseClientSettings, ClickhouseNode

MIGRATIONS_CONNECTIONS: MutableMapping[str, ClickhousePool] = {}


def get_ddl_node_connection(node: ClickhouseNode, database: str) -> ClickhousePool:
    """
    Get connection to a storage or query node in order to
    run the DDL statements defined by a SQL Operation in
    a migration.
    """
    key = f"{node.host_name}-{node.port}"
    if key in MIGRATIONS_CONNECTIONS:
        return MIGRATIONS_CONNECTIONS[key]

    connection = ClickhousePool(
        node.host_name,
        node.port,
        settings.CLICKHOUSE_MIGRATIONS_DDL_USER,
        settings.CLICKHOUSE_MIGRATIONS_DDL_PASSWORD,
        database,
        max_pool_size=2,
        client_settings=ClickhouseClientSettings.MIGRATE_DDL.value.settings,
    )
    MIGRATIONS_CONNECTIONS[key] = connection
    return connection
