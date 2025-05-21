import inspect
import json
from functools import wraps
from typing import Any, Callable

import sentry_sdk

from snuba import environment
from snuba.clusters.cluster import ClickhouseClientSettings, get_cluster
from snuba.clusters.storage_sets import StorageSetKey
from snuba.redis import RedisClientKey, get_redis_client
from snuba.utils.metrics.wrapper import MetricsWrapper

metrics = MetricsWrapper(
    environment.metrics,
    "snuba.web.rpc.v1.resolvers.R_eap_items.storage_routing.load_retriever",
)


class LoadInfo:
    cluster_load: float
    concurrent_queries: int

    def __init__(self, cluster_load: float, concurrent_queries: int) -> None:
        self.cluster_load = cluster_load
        self.concurrent_queries = concurrent_queries

    def to_dict(self) -> dict[str, float | int]:
        return {
            "cluster_load": self.cluster_load,
            "concurrent_queries": self.concurrent_queries,
        }

    @classmethod
    def from_dict(cls, load_info_dict: dict[str, float | int]) -> "LoadInfo":
        return cls(
            cluster_load=load_info_dict["cluster_load"],
            concurrent_queries=int(load_info_dict["concurrent_queries"]),
        )


def cache(
    *, ttl_secs: int = 60
) -> Callable[[Callable[..., LoadInfo]], Callable[..., LoadInfo]]:
    def decorator(func: Callable[..., LoadInfo]) -> Callable[..., LoadInfo]:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:

            bound_args = inspect.signature(func).bind(*args, **kwargs)
            bound_args.apply_defaults()

            cache_key_parts = [func.__name__]
            for param_name, param_value in bound_args.arguments.items():
                cache_key_parts.append(f"{param_name}:{param_value}")

            cache_key = ":".join(cache_key_parts)

            redis_client = get_redis_client(RedisClientKey.CACHE)

            # Try to fetch the result from Redis
            cached_result = redis_client.get(cache_key)
            if cached_result:
                return LoadInfo.from_dict(json.loads(cached_result))

            # If not in cache, call the function and cache the result
            result = func(*args, **kwargs)
            redis_client.set(cache_key, json.dumps(result.to_dict()), ex=ttl_secs)

            return result

        return wrapper

    return decorator


@cache(ttl_secs=60)
def get_cluster_loadinfo(
    storage_set_key: StorageSetKey = StorageSetKey.EVENTS_ANALYTICS_PLATFORM,
) -> LoadInfo:
    try:
        cluster = get_cluster(storage_set_key)
        cluster_name = str(cluster.get_clickhouse_cluster_name())

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
            concurrent_queries_query = f"""
            SELECT sum(toUInt64(count)) AS concurrent_queries
            FROM clusterAllReplicas('{cluster.get_clickhouse_cluster_name()}',
                (
                    SELECT toString(count()) AS count
                    FROM system.processes
                )
            );
            """

        print(f"cluster_load_query: {cluster_load_query}")
        print(f"concurrent_queries_query: {concurrent_queries_query}")

        cluster_load = float(
            cluster.get_query_connection(ClickhouseClientSettings.QUERY)
            .execute(cluster_load_query)
            .results[0][0]
        )
        print("clusterload query finished")
        concurrent_queries = int(
            cluster.get_query_connection(ClickhouseClientSettings.QUERY)
            .execute(concurrent_queries_query)
            .results[0][0]
        )
        print("concurrent queries query finished")
        load_info = LoadInfo(
            cluster_load=cluster_load, concurrent_queries=concurrent_queries
        )
        metrics.gauge(
            "cluster_load", load_info.cluster_load, tags={"cluster_name": cluster_name}
        )
        metrics.gauge(
            "concurrent_queries",
            load_info.concurrent_queries,
            tags={"cluster_name": cluster_name},
        )
        return load_info

    except Exception as e:
        print(f"Error getting clusterloadinfoo: {e}")
        metrics.increment("get_cluster_load_failure")
        sentry_sdk.capture_exception(e)
        return LoadInfo(cluster_load=-1.0, concurrent_queries=-1)
