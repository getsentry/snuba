from snuba import settings
from snuba.clickhouse.native import ClickhousePool, ClickhouseResult
from snuba.clusters.cluster import ClickhouseClientSettings
from snuba.datasets.schemas.tables import TableSchema
from snuba.datasets.storages.factory import get_storage
from snuba.datasets.storages.storage_key import StorageKey

HOUR_IN_SECONDS = 3600


"""
Generic Metrics
"""

GEN_METRICS_STORAGE_KEYS = [
    StorageKey.GENERIC_METRICS_COUNTERS_RAW,
    StorageKey.GENERIC_METRICS_DISTRIBUTIONS_RAW,
    StorageKey.GENERIC_METRICS_SETS_RAW,
]


def build_query(storage_key: StorageKey) -> str:
    schema = get_storage(storage_key).get_schema()
    assert isinstance(schema, TableSchema)
    table_name = schema.get_table_name()
    return f"SELECT use_case_id, count() FROM {table_name} WHERE timestamp > now() - {HOUR_IN_SECONDS} GROUP BY use_case_id"


def execute_query(query: str, storage_key: StorageKey) -> ClickhouseResult:
    storage = get_storage(storage_key)
    cluster = storage.get_cluster()
    connection_id = cluster.get_connection_id()
    database = cluster.get_database()

    username = settings.CLICKHOUSE_READONLY_USER
    password = settings.CLICKHOUSE_READONLY_PASSWORD

    connection = ClickhousePool(
        connection_id.hostname,
        connection_id.tcp_port,
        username,
        password,
        database,
        max_pool_size=2,
        client_settings=ClickhouseClientSettings.QUERY.value.settings,
    )

    query_result = connection.execute(
        query=query,
        with_column_types=True,
        settings={"max_threads": 1},
    )
    return query_result
