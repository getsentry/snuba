from __future__ import absolute_import

from redis.client import StrictRedis
from redis.exceptions import BusyLoadingError, ConnectionError
from rediscluster import StrictRedisCluster

from snuba import settings


class RetryingStrictRedisCluster(StrictRedisCluster):
    """
    Execute a command with cluster reinitialization retry logic.
    Should a cluster respond with a ConnectionError or BusyLoadingError the
    cluster nodes list will be reinitialized and the command will be executed
    again with the most up to date view of the world.
    """

    def execute_command(self, *args, **kwargs):
        try:
            return super(self.__class__, self).execute_command(*args, **kwargs)
        except (
            ConnectionError,
            BusyLoadingError,
            KeyError,  # see: https://github.com/Grokzen/redis-py-cluster/issues/287
        ):
            self.connection_pool.nodes.reset()
            return super(self.__class__, self).execute_command(*args, **kwargs)


if settings.USE_REDIS_CLUSTER:
    startup_nodes = settings.REDIS_CLUSTER_STARTUP_NODES
    if startup_nodes is None:
        startup_nodes = [{"host": settings.REDIS_HOST, "port": settings.REDIS_PORT}]
    redis_client = RetryingStrictRedisCluster(
        startup_nodes=startup_nodes,
        socket_keepalive=True,
        password=settings.REDIS_PASSWORD,
    )
else:
    redis_client = StrictRedis(
        host=settings.REDIS_HOST,
        port=settings.REDIS_PORT,
        password=settings.REDIS_PASSWORD,
        db=settings.REDIS_DB,
        socket_keepalive=True,
    )
