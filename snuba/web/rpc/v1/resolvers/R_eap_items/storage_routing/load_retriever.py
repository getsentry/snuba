import json

import sentry_sdk

from snuba import environment
from snuba.clusters.cluster import ClickhouseClientSettings, get_cluster
from snuba.clusters.storage_sets import StorageSetKey
from snuba.redis import RedisClientKey, get_redis_client
from snuba.utils.metrics.wrapper import MetricsWrapper

_redis_client = get_redis_client(RedisClientKey.CLUSTER_LOAD)
metrics = MetricsWrapper(environment.metrics, "snuba.web.rpc.v1.resolvers.R_eap_items.storage_routing.load_retriever")


class LoadInfo:
    cluster_load: float
    concurrent_queries: int

    def __init__(self, cluster_load: float, concurrent_queries: int) -> None:
        self.cluster_load = cluster_load
        self.concurrent_queries = concurrent_queries

    def to_dict(self) -> dict[str, float | int]:
        return {
            "cluster_load": self.cluster_load,
            "concurrent_queries": self.concurrent_queries
        }

    @classmethod
    def from_dict(cls, load_info_dict: dict[str, float | int]) -> "LoadInfo":
        return cls(
            cluster_load=load_info_dict["cluster_load"],
            concurrent_queries=int(load_info_dict["concurrent_queries"])
        )

def get_cluster_load(storage_set_key: StorageSetKey = StorageSetKey.EVENTS_ANALYTICS_PLATFORM) -> LoadInfo | None:
    try:
        cluster = get_cluster(storage_set_key)
        cluster_name = str(cluster.get_clickhouse_cluster_name())
        cached_load_info = _redis_client.get(cluster_name)
        if cached_load_info:
            deserialized_cached_load_info = json.loads(cached_load_info)
            return LoadInfo(
                cluster_load=deserialized_cached_load_info["cluster_load"],
                concurrent_queries=deserialized_cached_load_info["concurrent_queries"]
            )

        if cluster.is_single_node():
            cluster_load_query = """
    SELECT
        toFloat32(value)/ (SELECT
                    max(toInt32(replaceAll(metric, 'OSNiceTimeCPU', ''))) + 1 as num_cpus
                FROM system.asynchronous_metrics
                WHERE metric LIKE '%OSNiceTimeCPU%') * 100 as normalized_load
    FROM system.asynchronous_metrics
    WHERE metric = 'LoadAverage1'
            """
            concurrent_queries_query = """
            SELECT
                count()
            FROM system.processes
            """
        else:
            cluster_load_query = f"""
    SELECT
        max(load_average.value / cpu_counts.num_cpus * 100) AS max_normalized_load
    FROM (
        SELECT
            hostName() AS host,
            value,
            metric
        FROM clusterAllReplicas('{cluster.get_clickhouse_cluster_name()}', 'system', asynchronous_metrics)
        WHERE metric = 'LoadAverage1'
    ) AS load_average
    JOIN (
        SELECT
            hostName() AS host,
            max(toInt32(replaceAll(metric, 'OSNiceTimeCPU', ''))) + 1 AS num_cpus
        FROM clusterAllReplicas('{cluster.get_clickhouse_cluster_name()}', 'system', asynchronous_metrics)
        WHERE metric LIKE 'OSNiceTimeCPU%'
        GROUP BY host
    ) AS cpu_counts
    ON load_average.host = cpu_counts.host
        """
            concurrent_queries_query = """
            SELECT sum(count) AS concurrent_queries
            FROM clusterAllReplicas('{cluster.get_clickhouse_cluster_name()}',
                (
                    SELECT count() AS count
                    FROM system.processes
                )
            );
            """

        cluster_load = float(
            cluster.get_query_connection(ClickhouseClientSettings.QUERY)
            .execute(cluster_load_query)
            .results[0][0]
        )
        concurrent_queries = int(
            cluster.get_query_connection(ClickhouseClientSettings.QUERY)
            .execute(concurrent_queries_query)
            .results[0][0]
        )
        load_info = LoadInfo(cluster_load=cluster_load, concurrent_queries=concurrent_queries)
        _redis_client.set(cluster_name, json.dumps(load_info.to_dict()), ex=60)
        metrics.gauge("cluster_load", load_info.cluster_load, tags={"cluster_name": cluster_name})
        metrics.gauge("concurrent_queries", load_info.concurrent_queries, tags={"cluster_name": cluster_name})
        return load_info

    except Exception as e:
        metrics.increment("get_cluster_load_failure")
        sentry_sdk.capture_exception(e)
        return LoadInfo(cluster_load=-1.0, concurrent_queries=-1)
