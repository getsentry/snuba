from typing import cast

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


def _stringify_result(result: ClickhouseResult) -> ClickhouseResult:
    # Match cardinality analyzer: stringify cells so large integers are not
    # rounded by JavaScript's number type on the client.
    result_rows = []
    for row in result.results:
        result_rows.append([str(col) for col in row])
    return ClickhouseResult(result_rows, result.meta)


@audit_log
def run_outcomes_query(query: str, user: str) -> ClickhouseResult:
    """
    Validates, audit logs, and executes a read-only query against outcomes
    tables. `user` is required by the audit_log decorator.
    """
    storage_keys = {
        StorageKey("outcomes_hourly"),
        StorageKey("outcomes_daily"),
        StorageKey("outcomes_raw"),
    }
    schemas = {get_storage(storage_key).get_schema() for storage_key in storage_keys}
    allowed_tables = {cast(TableSchema, schema).get_table_name() for schema in schemas}
    # Prefer the hourly/daily distributed tables; raw is allowed but expensive.
    allowed_tables |= {
        "outcomes_hourly_dist",
        "outcomes_hourly_local",
        "outcomes_daily_dist",
        "outcomes_daily_dist_v2",
        "outcomes_daily_local",
        "outcomes_daily_local_v2",
        "outcomes_raw_dist",
        "outcomes_raw_local",
    }

    validate_ro_query(sql_query=query, allowed_tables=allowed_tables)
    return _stringify_result(__run_query(query))


def __run_query(query: str) -> ClickhouseResult:
    connection = get_ro_query_node_connection(
        StorageKey("outcomes_hourly").value,
        ClickhouseClientSettings.CARDINALITY_ANALYZER,
    )

    return connection.execute(
        query=query,
        with_column_types=True,
    )
