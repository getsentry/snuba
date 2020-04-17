from unittest import mock

from snuba.state.cache.abstract import Cache
from snuba.utils.codecs import PassthroughCodec
from snuba.state.cache.redis.backend import RedisCache
from snuba.redis import redis_client
from tests.assertions import assert_changes, assert_does_not_change


def test_get_or_execute() -> None:
    codec: PassthroughCodec[int] = PassthroughCodec()
    backend: Cache[int] = RedisCache(redis_client, "test", codec)

    key = "key"
    value = 1
    function = mock.MagicMock(return_value=value)

    assert backend.get(key) is None

    with assert_changes(lambda: function.call_count, 0, 1):
        backend.get_or_execute(key, function, 5) == value

    assert backend.get(key) == value

    with assert_does_not_change(lambda: function.call_count, 1):
        backend.get_or_execute(key, function, 5) == value
