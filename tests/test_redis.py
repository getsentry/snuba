from redis.exceptions import RedisClusterException

from snuba import redis


def test_retry_init() -> None:
    fails_left = 2

    @redis._retry(2)
    def my_bad_function() -> int:
        nonlocal fails_left
        fails_left -= 1
        if fails_left > 0:
            raise RedisClusterException("All slots are not covered after query all startup_nodes.")
        return 1

    assert my_bad_function() == 1
