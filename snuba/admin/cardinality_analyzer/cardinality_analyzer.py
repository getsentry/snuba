from snuba.admin.audit_log.query import audit_log
from snuba.admin.clickhouse.common import (
    get_ro_cluster_node_connection,
    validate_ro_query,
)
from snuba.clickhouse.pool import ClickhousePool, ClickhouseResult
from snuba.clusters.cluster import ClickhouseClientSettings, get_cluster
from snuba.clusters.storage_sets import StorageSetKey

# HACK (VOLO): Everything in this file is a hack


def _stringify_result(result: ClickhouseResult) -> ClickhouseResult:
    # javascript stores numbers as doubles so in order for it to not round
    # metric ids (which are all prefixed by (1 << 63)) we strigify them before sending
    # them to the client
    result_rows = []
    for row in result.results:
        result_rows.append([str(col) for col in row])
    return ClickhouseResult(result_rows, result.meta)


@audit_log
def run_metrics_query(query: str, user: str) -> ClickhouseResult:
    """
    Validates, audit logs, and executes given query against leftover generic
    metrics ClickHouse tables. `user` param is necessary for audit_log decorator.
    """
    allowed_tables = {
        "generic_metric_counters_raw_dist",
        "generic_metric_counters_aggregated_dist",
        "generic_metric_counters_meta_aggregated_dist",
        "generic_metric_counters_meta_tag_value_aggregated_dist",
        "generic_metric_counters_meta_dist",
        "generic_metric_counters_meta_tag_values_dist",
    }

    def get_connection() -> ClickhousePool:
        cluster = get_cluster(StorageSetKey.GENERIC_METRICS_COUNTERS)
        return get_ro_cluster_node_connection(
            cluster,
            cluster.get_query_node(),
            ClickhouseClientSettings.CARDINALITY_ANALYZER,
        )

    connection = validate_ro_query(
        sql_query=query,
        allowed_tables=allowed_tables,
        get_connection=get_connection,
    )
    assert connection is not None
    return _stringify_result(__run_query(query, connection))


def __run_query(query: str, connection: ClickhousePool) -> ClickhouseResult:
    """
    Runs given Query against metrics tables in ClickHouse. This function assumes valid
    query and does not validate/sanitize query or response data.
    """
    query_result = connection.execute(
        query=query,
        with_column_types=True,
    )
    return query_result
