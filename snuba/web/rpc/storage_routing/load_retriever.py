import inspect
import json
from collections.abc import Callable
from dataclasses import dataclass, fields
from functools import wraps
from typing import Any

import sentry_sdk

from snuba import environment
from snuba.clusters.cluster import ClickhouseClientSettings, get_cluster
from snuba.clusters.storage_sets import StorageSetKey
from snuba.redis import RedisClientKey, get_redis_client
from snuba.utils.metrics.wrapper import MetricsWrapper

metrics = MetricsWrapper(
    environment.metrics,
    "snuba.web.rpc.storage_routing.load_retriever",
)


@dataclass
class LoadInfo:
    cluster_load: float = -1.0
    concurrent_queries: float = -1.0
    cgroup_user_time_normalized: float = -1.0
    block_in_flight_ops: float = -1.0
    memory_tracking: float = -1.0
    part_mutation: float = -1.0

    def to_dict(self) -> dict[str, float]:
        return {f.name: getattr(self, f.name) for f in fields(self)}

    @classmethod
    def from_dict(cls, load_info_dict: dict[str, float | int | None]) -> "LoadInfo":
        return cls(
            **{
                f.name: float(v)
                for f in fields(cls)
                if (v := load_info_dict.get(f.name)) is not None
            }
        )


def cache(
    ttl_secs: int = 60,
) -> Callable[[Callable[..., LoadInfo]], Callable[..., LoadInfo]]:
    def decorator(func: Callable[..., LoadInfo]) -> Callable[..., LoadInfo]:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> LoadInfo:
            result = None
            try:
                bound_args = inspect.signature(func).bind(*args, **kwargs)
                bound_args.apply_defaults()

                cache_key_parts = [func.__name__]
                for param_name, param_value in bound_args.arguments.items():
                    cache_key_parts.append(f"{param_name}:{param_value}")

                cache_key = ":".join(cache_key_parts)

                redis_client = get_redis_client(RedisClientKey.CACHE)

                cached_result = redis_client.get(cache_key)
                if cached_result:
                    return LoadInfo.from_dict(json.loads(cached_result))

                # it is expected that func has error handling, so we don't need to handle it here
                result = func(*args, **kwargs)
                redis_client.set(cache_key, json.dumps(result.to_dict()), ex=ttl_secs)

                return result
            except Exception as e:
                metrics.increment("get_cluster_loadinfo_caching_failure")
                sentry_sdk.capture_exception(e)
                return result if result is not None else func(*args, **kwargs)

        return wrapper

    return decorator


_LOAD_FIELDS = tuple(f.name for f in fields(LoadInfo))


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
                toFloat32(max(if(metric = 'LoadAverage1', value, NULL)))
                    / (SELECT
                            max(toInt32(replaceAll(metric, 'OSNiceTimeCPU', ''))) + 1 as num_cpus
                        FROM system.asynchronous_metrics
                        WHERE metric LIKE '%OSNiceTimeCPU%') * 100 as cluster_load,
                (SELECT max(if(metric = 'Query', value, NULL)) FROM system.metrics
                    WHERE metric = 'Query') as concurrent_queries,
                max(if(metric = 'CGroupUserTimeNormalized', value, NULL)) as cgroup_user_time_normalized,
                max(if(metric = 'BlockInFlightOps', value, NULL)) as block_in_flight_ops,
                (SELECT max(if(metric = 'MemoryTracking', value, NULL)) FROM system.metrics
                    WHERE metric = 'MemoryTracking') as memory_tracking,
                (SELECT max(if(metric = 'PartMutation', value, NULL)) FROM system.metrics
                    WHERE metric = 'PartMutation') as part_mutation
            FROM system.asynchronous_metrics
            WHERE metric IN ('LoadAverage1', 'CGroupUserTimeNormalized', 'BlockInFlightOps')
            """
        else:
            replicas = f"clusterAllReplicas('{cluster.get_clickhouse_cluster_name()}'"
            cluster_load_query = f"""
            SELECT
                load.cluster_load,
                metrics.concurrent_queries,
                load.cgroup_user_time_normalized,
                load.block_in_flight_ops,
                metrics.memory_tracking,
                metrics.part_mutation
            FROM (
                SELECT
                    max(if(load_average.metric = 'LoadAverage1',
                           load_average.value / cpu_counts.num_cpus * 100, NULL)) AS cluster_load,
                    max(if(load_average.metric = 'CGroupUserTimeNormalized',
                           load_average.value, NULL)) AS cgroup_user_time_normalized,
                    max(if(load_average.metric = 'BlockInFlightOps',
                           load_average.value, NULL)) AS block_in_flight_ops
                FROM (
                    SELECT hostName() AS host, value, metric
                    FROM {replicas}, 'system', asynchronous_metrics)
                    WHERE metric IN ('LoadAverage1', 'CGroupUserTimeNormalized', 'BlockInFlightOps')
                ) AS load_average
                JOIN (
                    SELECT
                        hostName() AS host,
                        max(toInt32(replaceAll(metric, 'OSNiceTimeCPU', ''))) + 1 AS num_cpus
                    FROM {replicas}, 'system', asynchronous_metrics)
                    WHERE metric LIKE 'OSNiceTimeCPU%'
                    GROUP BY host
                ) AS cpu_counts
                ON load_average.host = cpu_counts.host
            ) AS load
            CROSS JOIN (
                SELECT
                    max(if(metric = 'Query', value, NULL)) AS concurrent_queries,
                    max(if(metric = 'MemoryTracking', value, NULL)) AS memory_tracking,
                    max(if(metric = 'PartMutation', value, NULL)) AS part_mutation
                FROM {replicas}, 'system', 'metrics')
                WHERE metric IN ('Query', 'MemoryTracking', 'PartMutation')
            ) AS metrics
            """

        row = (
            cluster.get_query_connection(ClickhouseClientSettings.INTERNAL)
            .execute(cluster_load_query)
            .results[0]
        )
        load_info = LoadInfo.from_dict(dict(zip(_LOAD_FIELDS, row, strict=True)))

        tags = {"cluster_name": cluster_name}
        for name, value in load_info.to_dict().items():
            metrics.gauge(name, value, tags=tags)
        return load_info

    except Exception as e:
        metrics.increment("get_cluster_loadinfo_failure", tags={"cluster_name": cluster_name})
        sentry_sdk.capture_exception(e)
        return LoadInfo()
