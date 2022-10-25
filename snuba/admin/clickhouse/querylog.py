from snuba.admin.clickhouse.common import (
    get_ro_query_node_connection,
    validate_ro_query,
)
from snuba.clickhouse.native import ClickhouseResult
from snuba.clusters.cluster import ClickhouseClientSettings


def run_querylog_query(query: str, user: str) -> ClickhouseResult:
    validate_ro_query(
        sql_query=query, allowed_tables=["querylog_local", "querylog_dist"]
    )
    connection = get_ro_query_node_connection(
        "querylog", ClickhouseClientSettings.QUERY
    )
    audit_log_query(query, user)
    query_result = connection.execute(query=query, with_column_types=True)
    return query_result


def audit_log_query(query: str, user: str) -> None:
    """
    Log query and user to GCP logs for auditlog
    """
    pass
