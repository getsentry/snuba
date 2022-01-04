from __future__ import absolute_import

import random
import time
from functools import wraps
from typing import Any, Callable, Union

from rediscluster import StrictRedisCluster
from rediscluster.exceptions import RedisClusterException

from redis.client import StrictRedis
from redis.exceptions import BusyLoadingError, ConnectionError
from snuba import settings
from snuba.utils.serializable_exception import SerializableException

RedisClientType = Union[StrictRedis, StrictRedisCluster]


class FailedClusterInitization(SerializableException):
    pass


class RetryingStrictRedisCluster(StrictRedisCluster):  # type: ignore #  Missing type stubs in client lib
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
            self.connection_pool.nodes.reset()
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
            retry_counter = max_retries
            while retry_counter > 0:
                try:
                    return func()
                except RedisClusterException as e:
                    if (
                        KNOWN_TRANSIENT_INIT_FAILURE_MESSAGE in str(e)
                        and retry_counter >= 0
                    ):
                        time.sleep(random.randint(1, RANDOM_SLEEP_MAX) / 1000)
                        retry_counter -= 1
                        continue
                    raise
            # shouldn't get to here but it pleases mypy
            raise FailedClusterInitization("Init failed")

        return wrapper

    return _retry_inner


@_retry(max_retries=settings.REDIS_INIT_MAX_RETRIES)
def _initialize_redis_cluster() -> RedisClientType:
    if settings.USE_REDIS_CLUSTER:
        startup_nodes = settings.REDIS_CLUSTER_STARTUP_NODES
        if startup_nodes is None:
            startup_nodes = [{"host": settings.REDIS_HOST, "port": settings.REDIS_PORT}]
        return RetryingStrictRedisCluster(
            startup_nodes=startup_nodes,
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
