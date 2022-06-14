from __future__ import absolute_import

import time
from functools import wraps
from typing import Any, Callable, Union, cast

from redis.client import StrictRedis
from redis.cluster import ClusterNode, NodesManager, RedisCluster, RedisClusterException
from redis.exceptions import BusyLoadingError, ConnectionError
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

RedisInitFunction = Callable[[], RedisClientType]


def _retry(max_retries: int) -> Callable[[RedisInitFunction], RedisInitFunction]:
    def _retry_inner(
        func: Callable[[], RedisClientType]
    ) -> Callable[[], RedisClientType]:
        @wraps(func)
        def wrapper() -> RedisClientType:
            retry_counter = 0
            while retry_counter < max_retries:
                try:
                    return func()
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

        return wrapper

    return _retry_inner


@_retry(max_retries=settings.REDIS_INIT_MAX_RETRIES)
def _initialize_redis_cluster() -> RedisClientType:
    if settings.USE_REDIS_CLUSTER:
        startup_nodes = settings.REDIS_CLUSTER_STARTUP_NODES
        if startup_nodes is None:
            startup_nodes = [{"host": settings.REDIS_HOST, "port": settings.REDIS_PORT}]
        startup_cluster_nodes = [
            ClusterNode(n["host"], n["port"]) for n in startup_nodes
        ]
        return RetryingStrictRedisCluster(
            startup_nodes=startup_cluster_nodes,
            socket_keepalive=True,
            password=settings.REDIS_PASSWORD,
            max_connections_per_node=True,
        )
    else:
        return StrictRedis(
            host=settings.REDIS_HOST,
            port=settings.REDIS_PORT,
            password=settings.REDIS_PASSWORD,
            db=settings.REDIS_DB,
            socket_keepalive=True,
        )


redis_client: RedisClientType = _initialize_redis_cluster()
