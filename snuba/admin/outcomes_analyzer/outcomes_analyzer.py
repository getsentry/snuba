from typing import cast

from snuba.admin.audit_log.query import audit_log
from snuba.admin.clickhouse.common import (
    get_ro_query_node_connection,
    validate_ro_query,
)
from snuba.clickhouse.pool import ClickhouseResult
from snuba.clusters.cluster import ClickhouseClientSettings
from snuba.datasets.schemas.tables import TableSchema
from snuba.datasets.storages.factory import get_storage
from snuba.datasets.storages.storage_key import StorageKey

_OUTCOMES_STORAGE_KEYS = (
    StorageKey("outcomes_hourly"),
    StorageKey("outcomes_daily"),
    StorageKey("outcomes_raw"),
)


def _stringify_result(result: ClickhouseResult) -> ClickhouseResult:
    # Stringify cells so large integers are not rounded by JS Number on the client.
    return ClickhouseResult(
        [[str(col) for col in row] for row in result.results],
        result.meta,
    )


def _allowed_tables() -> set[str]:
    tables: set[str] = set()
    for storage_key in _OUTCOMES_STORAGE_KEYS:
        schema = cast(TableSchema, get_storage(storage_key).get_schema())
        tables.add(schema.get_local_table_name())
        tables.add(schema.get_dist_table_name())
    return tables


@audit_log
def run_outcomes_query(query: str, user: str) -> ClickhouseResult:
    """
    Validates, audit logs, and executes a read-only query against outcomes
    tables. `user` is required by the audit_log decorator.
    """
    connection = get_ro_query_node_connection(
        StorageKey("outcomes_hourly").value,
        ClickhouseClientSettings.CARDINALITY_ANALYZER,
    )
    validate_ro_query(sql_query=query, allowed_tables=_allowed_tables(), connection=connection)
    return _stringify_result(connection.execute(query=query, with_column_types=True))
