from typing import Callable

from sentry_protos.snuba.v1.endpoint_time_series_pb2 import TimeSeriesRequest
from sentry_protos.snuba.v1.endpoint_trace_item_table_pb2 import TraceItemTableRequest

from snuba.clusters.cluster import ClickhouseClientSettings
from snuba.datasets.storages.factory import get_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.query.logical import Query
from snuba.query.query_settings import HTTPQuerySettings
from snuba.utils.metrics.timer import Timer
from snuba.utils.metrics.util import with_span
from snuba.web import QueryResult
from snuba.web.rpc.v1.resolvers.R_eap_items.storage_routing.routing_strategies.storage_routing import (
    RoutingContext,
)
from snuba.web.rpc.v1.resolvers.R_eap_items.storage_routing.routing_strategy_selector import (
    RoutingStrategySelector,
)


@with_span(op="function")
def run_query_to_correct_tier(
    in_msg: TraceItemTableRequest | TimeSeriesRequest,
    query_settings: HTTPQuerySettings,
    timer: Timer,
    build_query: Callable[[TraceItemTableRequest | TimeSeriesRequest], Query],
) -> QueryResult:
    routing_context = RoutingContext(
        in_msg=in_msg,
        timer=timer,
        build_query=build_query,
        query_settings=query_settings,
    )

    selected_strategy = RoutingStrategySelector().select_routing_strategy(
        routing_context
    )
    return selected_strategy.run_query_to_correct_tier(routing_context)


def get_eap_cluster_load() -> float:
    eap_cluster = get_storage(StorageKey.EAP_ITEMS).get_cluster()
    if eap_cluster.is_single_node():
        query = """
SELECT
    toFloat32(value)/ (SELECT
                max(toInt32(replaceAll(metric, 'OSNiceTimeCPU', ''))) + 1 as num_cpus
            FROM system.asynchronous_metrics
            WHERE metric LIKE '%OSNiceTimeCPU%') * 100 as normalized_load
FROM system.asynchronous_metrics
WHERE metric = 'LoadAverage1'
        """
    else:
        query = f"""
SELECT
    max(load_average.value / cpu_counts.num_cpus * 100) AS max_normalized_load
FROM (
    SELECT
        hostName() AS host,
        value,
        metric
    FROM clusterAllReplicas('{eap_cluster.get_clickhouse_cluster_name()}', 'system', asynchronous_metrics)
    WHERE metric = 'LoadAverage1'
) AS load_average
JOIN (
    SELECT
        hostName() AS host,
        max(toInt32(replaceAll(metric, 'OSNiceTimeCPU', ''))) + 1 AS num_cpus
    FROM clusterAllReplicas('{eap_cluster.get_clickhouse_cluster_name()}', 'system', asynchronous_metrics)
    WHERE metric LIKE 'OSNiceTimeCPU%'
    GROUP BY host
) AS cpu_counts
ON load_average.host = cpu_counts.host
    """

    return float(
        eap_cluster.get_query_connection(ClickhouseClientSettings.QUERY)
        .execute(query)
        .results[0][0]
    )
