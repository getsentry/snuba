from __future__ import annotations

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


@dataclass(kw_only=True, slots=True, frozen=True)
class LoadInfo:
    cluster_load: float = -1.0
    concurrent_queries: float = -1.0
    cgroup_user_time_normalized: float = -1.0
    disk_inflight_ops: float = -1.0
    memory_allocated: float = -1.0
    part_mutation: float = -1.0

    def to_dict(self) -> dict[str, float]:
        return {f.name: getattr(self, f.name) for f in fields(self)}

    @classmethod
    def from_dict(cls, load_info_dict: dict[str, float | int | None]) -> LoadInfo:
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


@cache(ttl_secs=60)
def get_cluster_loadinfo(
    storage_set_key: StorageSetKey = StorageSetKey.EVENTS_ANALYTICS_PLATFORM,
) -> LoadInfo:
    cluster_name = None
    try:
        cluster = get_cluster(storage_set_key)
        cluster_name = str(cluster.get_clickhouse_cluster_name())

        if cluster.is_single_node():
            metrics_from = """
                SELECT hostName() AS host, metric, toFloat64(value) AS value FROM system.asynchronous_metrics
                UNION ALL
                SELECT hostName() AS host, metric, toFloat64(value) AS value FROM system.metrics
            """
        else:
            metrics_from = f"""
                SELECT hostName() AS host, metric, toFloat64(value) AS value
                FROM clusterAllReplicas('{cluster_name}', 'system', asynchronous_metrics)
                UNION ALL
                SELECT hostName() AS host, metric, toFloat64(value) AS value
                FROM clusterAllReplicas('{cluster_name}', 'system', metrics)
            """

        # maxIf() returns 0 when the condition never matches; 0 is a real idle value
        # (Query/PartMutation). max(if(..., NULL)) stays NULL.
        # https://clickhouse.com/docs/sql-reference/aggregate-functions/combinators#-if
        cluster_load_query = f"""
        SELECT
            max(cluster_load) AS cluster_load,
            max(concurrent_queries) AS concurrent_queries,
            max(cgroup_user_time_normalized) AS cgroup_user_time_normalized,
            max(disk_inflight_ops) AS disk_inflight_ops,
            max(memory_allocated) AS memory_allocated,
            max(part_mutation) AS part_mutation
        FROM (
            SELECT
                ifNull(
                    max(if(metric = 'LoadAverage1', value, NULL))
                        / (max(if(
                            startsWith(metric, 'OSNiceTimeCPU'),
                            toInt32OrZero(replaceAll(metric, 'OSNiceTimeCPU', '')),
                            NULL
                        )) + 1)
                        * 100,
                    -1
                ) AS cluster_load,
                max(if(metric = 'Query', value, NULL)) AS concurrent_queries,
                max(if(metric = 'CGroupUserTimeNormalized', value, NULL)) AS cgroup_user_time_normalized,
                -- Actual metric is BlockInFlightOps_<device name>
                max(if(startsWith(metric, 'BlockInFlightOps'), value, NULL)) AS disk_inflight_ops,
                max(if(metric = 'MemoryTracking', value, NULL)) AS memory_allocated,
                max(if(metric = 'PartMutation', value, NULL)) AS part_mutation
            FROM (
                {metrics_from}
            )
            WHERE metric IN (
                'LoadAverage1', 'CGroupUserTimeNormalized',
                'Query', 'MemoryTracking', 'PartMutation'
            )
               OR startsWith(metric, 'OSNiceTimeCPU')
               OR startsWith(metric, 'BlockInFlightOps')
            GROUP BY host
        )
        """

        row = (
            cluster.get_query_connection(ClickhouseClientSettings.INTERNAL)
            .execute(cluster_load_query)
            .results[0]
        )
        load_info = LoadInfo.from_dict(
            {
                "cluster_load": row[0],
                "concurrent_queries": row[1],
                "cgroup_user_time_normalized": row[2],
                "disk_inflight_ops": row[3],
                "memory_allocated": row[4],
                "part_mutation": row[5],
            }
        )

        tags = {"cluster_name": cluster_name}
        for name, value in load_info.to_dict().items():
            metrics.gauge(name, value, tags=tags)
        return load_info

    except Exception as e:
        metrics.increment(
            "get_cluster_loadinfo_failure", tags={"cluster_name": cluster_name or "unknown"}
        )
        sentry_sdk.capture_exception(e)
        return LoadInfo()
