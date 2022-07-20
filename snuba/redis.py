from __future__ import absolute_import

import time
from functools import wraps
from typing import Any, Callable, Mapping, MutableMapping, Sequence, Union, cast

from redis.client import StrictRedis
from redis.cluster import ClusterNode, NodesManager, RedisCluster, RedisClusterException
from redis.exceptions import BusyLoadingError, ConnectionError
from snuba import settings
from snuba.redis_multi.configuration import (
    ClusterFunction,
    ConnectionDescriptor,
    NodeDescriptor,
)
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

RedisInitFunction = Callable[[ConnectionDescriptor], RedisClientType]


def _retry(max_retries: int) -> Callable[[RedisInitFunction], RedisInitFunction]:
    def _retry_inner(
        func: Callable[[ConnectionDescriptor], RedisClientType]
    ) -> Callable[[ConnectionDescriptor], RedisClientType]:
        @wraps(func)
        def wrapper(connection_desc: ConnectionDescriptor) -> RedisClientType:
            retry_counter = 0
            while retry_counter < max_retries:
                try:
                    return func(connection_desc)
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


_EXISTING_CLIENTS_BY_FUNCTION: MutableMapping[ClusterFunction, RedisClientType] = {}
_EXISTING_CLIENTS_BY_CONNECTION_PARAMS: MutableMapping[
    ConnectionDescriptor, RedisClientType
] = {}

_cluster_list_initialized = False


@_retry(max_retries=settings.REDIS_INIT_MAX_RETRIES)
def _build_single_node_client(
    connection_descriptor: ConnectionDescriptor,
) -> RedisClientType:
    assert isinstance(connection_descriptor, NodeDescriptor)
    return StrictRedis(
        host=connection_descriptor.host,
        port=connection_descriptor.port,
        password=connection_descriptor.password,
        db=connection_descriptor.db,
        socket_keepalive=True,
    )


@_retry(max_retries=settings.REDIS_INIT_MAX_RETRIES)
def _build_cluster_client(
    connection_descriptor: ConnectionDescriptor,
) -> RedisClientType:
    assert isinstance(connection_descriptor, Sequence)
    assert len(connection_descriptor) > 0
    assert isinstance(connection_descriptor[0], NodeDescriptor)
    client = RetryingStrictRedisCluster(
        startup_nodes=[
            ClusterNode(host=node.host, port=node.port)
            for node in connection_descriptor
        ],
        db=connection_descriptor[0].db,
        password=connection_descriptor[0].password,
        max_connections_per_node=True,
    )

    return client


def _initialize_isolated_clusters(
    redis_cluster_map: Mapping[ClusterFunction, ConnectionDescriptor]
) -> None:
    global _cluster_list_initialized
    if _cluster_list_initialized:
        return
    for func in ClusterFunction:
        assert (
            redis_cluster_map[func] is not None
        ), f"no cluster defined for cluster function {func}"
        connection_descriptor = redis_cluster_map[func]
        if connection_descriptor not in _EXISTING_CLIENTS_BY_CONNECTION_PARAMS:
            if isinstance(connection_descriptor, NodeDescriptor):
                client = _build_single_node_client(connection_descriptor)
                _EXISTING_CLIENTS_BY_CONNECTION_PARAMS[connection_descriptor] = client
                _EXISTING_CLIENTS_BY_FUNCTION[func] = client
            elif isinstance(connection_descriptor, Sequence):
                client = _build_cluster_client(connection_descriptor)
                _EXISTING_CLIENTS_BY_CONNECTION_PARAMS[connection_descriptor] = client
                _EXISTING_CLIENTS_BY_FUNCTION[func] = client
        else:
            _EXISTING_CLIENTS_BY_FUNCTION[
                func
            ] = _EXISTING_CLIENTS_BY_CONNECTION_PARAMS[connection_descriptor]
    _cluster_list_initialized = True


def get_redis_client(func: ClusterFunction) -> RedisClientType:
    _initialize_isolated_clusters(settings.REDIS_CLUSTER_MAP)
    return _EXISTING_CLIENTS_BY_FUNCTION[func]


# TODO This no-function-specified redis client should be deprecated but
# until we actually have separate clusters we can leave it in so we don't
# have to blow everything up in one PR.
redis_client: RedisClientType = get_redis_client(ClusterFunction.CACHE)
