from snuba.admin.clickhouse.common import (
    get_ro_query_node_connection,
    validate_ro_query,
)
from snuba.clickhouse.native import ClickhouseResult
from snuba.clusters.cluster import ClickhouseClientSettings


def run_query_and_get_trace(storage_name: str, query: str) -> ClickhouseResult:
    validate_ro_query(query)

    connection = get_ro_query_node_connection(
        storage_name, ClickhouseClientSettings.QUERY_AND_SETTINGS
    )
    query_result = connection.execute(query=query, capture_trace=True)
    return query_result
