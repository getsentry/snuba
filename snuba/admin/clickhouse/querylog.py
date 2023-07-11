from snuba import state
from snuba.admin.audit_log.query import audit_log
from snuba.admin.clickhouse.common import (
    get_ro_query_node_connection,
    validate_ro_query,
)
from snuba.clickhouse.native import ClickhouseResult
from snuba.clusters.cluster import ClickhouseClientSettings
from snuba.datasets.schemas.tables import TableSchema
from snuba.datasets.storages.factory import get_storage
from snuba.datasets.storages.storage_key import StorageKey

_MAX_CH_THREADS = 4


class BadThreadsValue(Exception):
    pass


@audit_log
def run_querylog_query(query: str, user: str) -> ClickhouseResult:
    """
    Validates, audit logs, and executes given query against Querylog
    table in ClickHouse. `user` param is necessary for audit_log
    decorator.
    """
    schema = get_storage(StorageKey.QUERYLOG).get_schema()
    assert isinstance(schema, TableSchema)
    validate_ro_query(
        sql_query=query, allowed_tables={schema.get_table_name(), "clickhouse_queries"}
    )
    return __run_querylog_query(query)


def describe_querylog_schema() -> ClickhouseResult:
    schema = get_storage(StorageKey.QUERYLOG).get_schema()
    assert isinstance(schema, TableSchema)
    return __run_querylog_query(f"DESCRIBE TABLE {schema.get_table_name()}")


def _get_clickhouse_threads() -> int:
    config_threads = state.get_config("admin.querylog_threads", _MAX_CH_THREADS)
    try:
        return min(
            int(config_threads) if config_threads is not None else _MAX_CH_THREADS,
            _MAX_CH_THREADS,
        )
    except ValueError:
        # in case the config is set incorrectly
        raise BadThreadsValue(
            f"{config_threads} is not a valid configuration option for Clickhouse `max_threads`"
        )


def __run_querylog_query(query: str) -> ClickhouseResult:
    """
    Runs given Query against Querylog table in ClickHouse. This function assumes valid
    query and does not validate/sanitize query or response data.
    """
    connection = get_ro_query_node_connection(
        StorageKey.QUERYLOG.value, ClickhouseClientSettings.QUERYLOG
    )

    query_result = connection.execute(
        query=query,
        with_column_types=True,
        settings={"max_threads": _get_clickhouse_threads()},
    )
    return query_result
