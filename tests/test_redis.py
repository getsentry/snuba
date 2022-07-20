from redis.exceptions import RedisClusterException  # type: ignore
from snuba import redis
from snuba.redis_multi.configuration import ConnectionDescriptor, NodeDescriptor


def test_retry_init() -> None:
    fails_left = 2

    @redis._retry(2)
    def my_bad_function(connection_descriptor: ConnectionDescriptor) -> int:
        nonlocal fails_left
        fails_left -= 1
        if fails_left > 0:
            raise RedisClusterException(
                "All slots are not covered after query all startup_nodes."
            )
        return 1

    assert my_bad_function(NodeDescriptor("localhost", 1234, 1, None)) == 1
