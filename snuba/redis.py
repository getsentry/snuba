from __future__ import absolute_import, annotations

import time
from functools import wraps
from typing import Any, Callable, TypedDict, TypeVar, Union, cast

from redis.client import StrictRedis
from redis.cluster import ClusterNode, NodesManager, RedisCluster
from redis.exceptions import BusyLoadingError, ConnectionError, RedisClusterException
from snuba import settings
from snuba.utils.serializable_exception import SerializableException

RedisClientType = Union[StrictRedis, RedisCluster]


class FailedClusterInitization(SerializableException):
    pass


class RetryingStrictRedisCluster(RedisCluster):  # type: ignore #  Missing type stubs in client lib
    """
    Execute a command with cluster reinitialization retry logic.
    Should a cluster respond with a ConnectionError or BusyLoadingError the
    cluster nodes list will be reinitialized and the command will be executed
    again with the most up to date view of the world.
    """

    def execute_command(self, *args: Any, **kwargs: Any) -> Any:
        try:
            return super(self.__class__, self).execute_command(*args, **kwargs)
        except (
            ConnectionError,
            BusyLoadingError,
            KeyError,  # see: https://github.com/Grokzen/redis-py-cluster/issues/287
        ):
            # the code in the RedisCluster __init__ idiotically sets
            # self.nodes_manager = None
            # self.nodes_manager = NodesManager(...)
            cast(NodesManager, self.nodes_manager).reset()
            return super(self.__class__, self).execute_command(*args, **kwargs)


RANDOM_SLEEP_MAX = 50
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
        return RetryingStrictRedisCluster(
            startup_nodes=startup_cluster_nodes,
            socket_keepalive=True,
            password=config["password"],
            max_connections_per_node=True,
            reinitialize_steps=config["reinitialize_steps"],
        )
    else:
        return StrictRedis(
            host=config["host"],
            port=config["port"],
            password=config["password"],
            db=config["db"],
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
        "reinitialize_steps": settings.REDIS_REINITIALIZE_STEPS,
    }
)


def _initialize_specialized_redis_cluster(
    config: settings.RedisClusterConfig | None,
) -> RedisClientType:
    if config is None:
        return _default_redis_client

    return _initialize_redis_cluster(config)


class RedisClients(TypedDict):
    cache: RedisClientType
    rate_limiter: RedisClientType
    subscription_store: RedisClientType
    replacements_store: RedisClientType
    misc: RedisClientType


redis_clients: RedisClients = {
    "cache": _initialize_specialized_redis_cluster(settings.REDIS_CLUSTERS["cache"]),
    "rate_limiter": _initialize_specialized_redis_cluster(
        settings.REDIS_CLUSTERS["rate_limiter"]
    ),
    "subscription_store": _initialize_specialized_redis_cluster(
        settings.REDIS_CLUSTERS["subscription_store"]
    ),
    "replacements_store": _initialize_specialized_redis_cluster(
        settings.REDIS_CLUSTERS["replacements_store"]
    ),
    "misc": _initialize_specialized_redis_cluster(settings.REDIS_CLUSTERS["misc"]),
}
