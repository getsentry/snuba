import time
from typing import Iterator
from unittest import mock

import pytest

from snuba.redis import redis_client
from snuba.state.cache.abstract import Cache
from snuba.state.cache.redis.backend import RedisCache
from snuba.utils.codecs import PassthroughCodec
from tests.assertions import assert_changes, assert_does_not_change


@pytest.fixture
def backend() -> Iterator[Cache[bytes]]:
    codec: PassthroughCodec[bytes] = PassthroughCodec()
    backend: Cache[bytes] = RedisCache(redis_client, "test", codec)
    try:
        yield backend
    finally:
        redis_client.flushdb()


def test_get_or_execute(backend: Cache[bytes]) -> None:
    key = "key"
    value = b"value"
    function = mock.MagicMock(return_value=value)

    assert backend.get(key) is None

    with assert_changes(lambda: function.call_count, 0, 1):
        backend.get_or_execute(key, function, 5) == value

    assert backend.get(key) == value

    with assert_does_not_change(lambda: function.call_count, 1):
        backend.get_or_execute(key, function, 5) == value


def test_get_or_execute_missed_deadline(backend: Cache[bytes]) -> None:
    key = "key"
    value = b"value"

    def function() -> bytes:
        time.sleep(1.5)
        return value

    backend.get_or_execute(key, function, 1) == value

    assert backend.get(key) is None


def test_get_or_execute_exception(backend: Cache[bytes]) -> None:
    key = "key"

    class CustomException(Exception):
        pass

    def function() -> bytes:
        raise CustomException("error")

    with pytest.raises(CustomException):
        backend.get_or_execute(key, function, 1)

    assert backend.get(key) is None
