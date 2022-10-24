from snuba.admin.clickhouse.common import (
    get_ro_query_node_connection,
    validate_ro_query,
)
from snuba.clickhouse.native import ClickhouseResult
from snuba.clusters.cluster import ClickhouseClientSettings


def run_querylog_query(query: str) -> ClickhouseResult:
    validate_ro_query(query)
    connection = get_ro_query_node_connection(
        "querylog", ClickhouseClientSettings.QUERY
    )
    query_result = connection.execute(query=query, with_column_types=True)
    return query_result
