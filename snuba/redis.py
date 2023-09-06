from __future__ import absolute_import, annotations

import time
from enum import Enum
from functools import wraps
from typing import Any, Callable, Iterable, Mapping, TypeVar, Union, cast

from sentry_redis_tools.failover_redis import FailoverRedis
from sentry_redis_tools.retrying_cluster import RetryingRedisCluster

from redis.cluster import ClusterNode, RedisCluster
from redis.exceptions import RedisClusterException
from snuba import settings
from snuba.utils.serializable_exception import SerializableException

# We use FailoverRedis as our default redis client for single-node deployments,
# as its additions to StrictRedis are required to work correctly under GCP
# memorystore. In case it isn't memorystore, there's not much harm in using
# FailoverRedis anyway.
SingleNodeRedis = FailoverRedis

RedisClientType = Union[SingleNodeRedis, RedisCluster]


class FailedClusterInitization(SerializableException):
    pass


KNOWN_TRANSIENT_INIT_FAILURE_MESSAGE = "All slots are not"

RedisInitFunction = TypeVar("RedisInitFunction", bound=Callable[..., Any])


def _retry(max_retries: int) -> Callable[[RedisInitFunction], RedisInitFunction]:
    def _retry_inner(func: RedisInitFunction) -> RedisInitFunction:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            retry_counter = 0
            while retry_counter < max_retries:
                try:
                    return func(*args, **kwargs)
                except RedisClusterException as e:
                    if KNOWN_TRANSIENT_INIT_FAILURE_MESSAGE in str(e):
                        # Exponentially increase the sleep starting from
                        # 0.5 seconds on each retry
                        sleep_duration = 0.5 * (2**retry_counter)
                        time.sleep(sleep_duration)
                        retry_counter += 1
                        continue
                    raise
            raise FailedClusterInitization("Init failed")

        return cast(RedisInitFunction, wrapper)

    return _retry_inner


@_retry(max_retries=settings.REDIS_INIT_MAX_RETRIES)
def _initialize_redis_cluster(config: settings.RedisClusterConfig) -> RedisClientType:
    if config["use_redis_cluster"]:
        startup_nodes = config["cluster_startup_nodes"]
        if startup_nodes is None:
            startup_nodes = [{"host": config["host"], "port": config["port"]}]
        startup_cluster_nodes = [
            ClusterNode(n["host"], n["port"]) for n in startup_nodes
        ]
        return RetryingRedisCluster(
            startup_nodes=startup_cluster_nodes,
            socket_keepalive=True,
            password=config["password"],
            max_connections_per_node=True,
            reinitialize_steps=config["reinitialize_steps"],
        )
    else:
        return SingleNodeRedis(
            host=config["host"],
            port=config["port"],
            password=config["password"],
            db=config["db"],
            ssl=config.get("ssl", False),
            socket_keepalive=True,
        )


_default_redis_client: RedisClientType = _initialize_redis_cluster(
    {
        "use_redis_cluster": settings.USE_REDIS_CLUSTER,
        "cluster_startup_nodes": settings.REDIS_CLUSTER_STARTUP_NODES,
        "host": settings.REDIS_HOST,
        "port": settings.REDIS_PORT,
        "password": settings.REDIS_PASSWORD,
        "db": settings.REDIS_DB,
        "ssl": settings.REDIS_SSL,
        "reinitialize_steps": settings.REDIS_REINITIALIZE_STEPS,
    }
)


def _initialize_specialized_redis_cluster(
    config: settings.RedisClusterConfig | None,
) -> RedisClientType:
    if config is None:
        return _default_redis_client

    return _initialize_redis_cluster(config)


class RedisClientKey(Enum):
    CACHE = "cache"
    RATE_LIMITER = "rate_limiter"
    SUBSCRIPTION_STORE = "subscription_store"
    REPLACEMENTS_STORE = "replacements_store"
    CONFIG = "config"
    DLQ = "dlq"
    OPTIMIZE = "optimize"
    ADMIN_AUTH = "admin_auth"
    ASYNC_QUERIES = "async_queries"


_redis_clients: Mapping[RedisClientKey, RedisClientType] = {
    RedisClientKey.CACHE: _initialize_specialized_redis_cluster(
        settings.REDIS_CLUSTERS["cache"]
    ),
    RedisClientKey.RATE_LIMITER: _initialize_specialized_redis_cluster(
        settings.REDIS_CLUSTERS["rate_limiter"]
    ),
    RedisClientKey.SUBSCRIPTION_STORE: _initialize_specialized_redis_cluster(
        settings.REDIS_CLUSTERS["subscription_store"]
    ),
    RedisClientKey.REPLACEMENTS_STORE: _initialize_specialized_redis_cluster(
        settings.REDIS_CLUSTERS["replacements_store"]
    ),
    RedisClientKey.CONFIG: _initialize_specialized_redis_cluster(
        settings.REDIS_CLUSTERS["config"]
    ),
    RedisClientKey.DLQ: _initialize_specialized_redis_cluster(
        settings.REDIS_CLUSTERS["dlq"]
    ),
    RedisClientKey.OPTIMIZE: _initialize_specialized_redis_cluster(
        settings.REDIS_CLUSTERS["optimize"]
    ),
    RedisClientKey.ADMIN_AUTH: _initialize_specialized_redis_cluster(
        settings.REDIS_CLUSTERS["admin_auth"]
    ),
    RedisClientKey.ADMIN_AUTH: _initialize_specialized_redis_cluster(
        settings.REDIS_CLUSTERS["async_queries"]
    ),
}


def get_redis_client(name: RedisClientKey) -> RedisClientType:
    return _redis_clients[name]


def all_redis_clients() -> Iterable[RedisClientType]:
    return cast(Iterable[RedisClientType], _redis_clients.values())
