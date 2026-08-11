from snuba.admin.audit_log.query import audit_log
from snuba.admin.clickhouse.common import (
    get_ro_query_node_connection,
    validate_ro_query,
)
from snuba.clickhouse.native import ClickhouseResult, Params
from snuba.clusters.cluster import ClickhouseClientSettings
from snuba.datasets.schemas.tables import TableSchema
from snuba.datasets.storages.factory import get_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.state.sentry_options import get_option

# Default cap for interactive querylog SQL. 0 means "use all cores" in ClickHouse
# and is reserved for intentional full-scan tools (EAP Stats).
_MAX_CH_THREADS = 4


@audit_log
def run_querylog_query(
    query: str,
    user: str,
    max_threads: int | None = None,
    params: Params = None,
) -> ClickhouseResult:
    """
    Validates, audit logs, and executes given query against Querylog
    table in ClickHouse. `user` param is necessary for audit_log
    decorator.

    ``max_threads`` overrides the default thread cap. Pass ``0`` to let
    ClickHouse use all available cores (needed for whole-dataset scans).
    """
    schema = get_storage(StorageKey.QUERYLOG).get_schema()
    assert isinstance(schema, TableSchema)
    validate_ro_query(
        sql_query=query, allowed_tables={schema.get_table_name(), "clickhouse_queries"}
    )
    return __run_querylog_query(query, max_threads=max_threads, params=params)


def describe_querylog_schema() -> ClickhouseResult:
    schema = get_storage(StorageKey.QUERYLOG).get_schema()
    assert isinstance(schema, TableSchema)
    return __run_querylog_query(f"DESCRIBE TABLE {schema.get_table_name()}")


def _get_clickhouse_threads(max_threads: int | None = None) -> int:
    if max_threads is not None:
        # Allow 0 (ClickHouse: use all cores). Negative values are invalid.
        return max(0, int(max_threads))
    config_threads = get_option("admin.querylog_threads", _MAX_CH_THREADS)
    return min(config_threads, _MAX_CH_THREADS)


def __run_querylog_query(
    query: str, max_threads: int | None = None, params: Params = None
) -> ClickhouseResult:
    """
    Runs given Query against Querylog table in ClickHouse. This function assumes valid
    query and does not validate/sanitize query or response data.
    """
    connection = get_ro_query_node_connection(
        StorageKey.QUERYLOG.value, ClickhouseClientSettings.QUERYLOG
    )

    query_result = connection.execute(
        query=query,
        params=params,
        with_column_types=True,
        settings={"max_threads": _get_clickhouse_threads(max_threads)},
    )
    return query_result
