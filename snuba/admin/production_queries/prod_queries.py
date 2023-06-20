from snuba.admin.audit_log.query import audit_log
from snuba.admin.clickhouse.common import (
    get_ro_query_node_connection,
    validate_ro_query,
)
from snuba.clickhouse.native import ClickhouseResult
from snuba.clusters.cluster import ClickhouseClientSettings


def run_prod_query(query: str, user: str, storage_name: str) -> ClickhouseResult:
    """
    Validates, audit logs, and executes given query against Querylog
    table in ClickHouse. `user` param is necessary for audit_log
    decorator.
    """
    validate_ro_query(query)
    connection = get_ro_query_node_connection(
        storage_name, ClickhouseClientSettings.QUERY
    )

    @audit_log
    def run_query_with_audit(query: str, user: str) -> ClickhouseResult:
        return connection.execute(query=query, with_column_types=True)

    return run_query_with_audit(query, user)
