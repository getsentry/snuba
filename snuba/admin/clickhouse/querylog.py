from snuba.admin.clickhouse.common import (
    InvalidCustomQuery,
    get_ro_query_node_connection,
    validate_ro_query,
)
from snuba.clickhouse.native import ClickhouseResult
from snuba.clusters.cluster import ClickhouseClientSettings
from snuba.datasets.schemas.tables import TableSchema
from snuba.datasets.storages.factory import get_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.state import get_config

ENABLE_QUERYLOG_API_CONFIG = "enable_clickhouse_querylog_api"


def run_querylog_query(query: str, user: str) -> ClickhouseResult:
    """
    Validates, audit logs, and executes given query against Querylog
    table in ClickHouse.
    """
    schema = get_storage(StorageKey.QUERYLOG).get_schema()
    assert isinstance(schema, TableSchema)
    validate_ro_query(sql_query=query, allowed_tables=[schema.get_table_name()])
    if get_config(ENABLE_QUERYLOG_API_CONFIG, 0):
        audit_log_query(query, user)
        return __run_querylog_query(query)
    raise InvalidCustomQuery("Production ClickHouse querylog access is not yet ready.")


def describe_querylog_schema() -> ClickhouseResult:
    schema = get_storage(StorageKey.QUERYLOG).get_schema()
    assert isinstance(schema, TableSchema)
    if get_config(ENABLE_QUERYLOG_API_CONFIG, 0):
        return __run_querylog_query(f"DESCRIBE TABLE {schema.get_table_name()}")
    raise InvalidCustomQuery("Production ClickHouse querylog access is not yet ready.")


def __run_querylog_query(query: str) -> ClickhouseResult:
    """
    Runs given Query against Querylog table in ClickHouse. This function assumes valid
    query and does not validate/sanitize query or response data.
    """
    connection = get_ro_query_node_connection(
        StorageKey.QUERYLOG.value, ClickhouseClientSettings.QUERY
    )
    query_result = connection.execute(query=query, with_column_types=True)
    return query_result


def audit_log_query(query: str, user: str) -> None:
    pass
