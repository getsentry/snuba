import random
import time
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from typing import Iterator
from unittest import mock

import pytest

from snuba.redis import redis_client
from snuba.state.cache.abstract import Cache, ExecutionError, ExecutionTimeoutError
from snuba.state.cache.redis.backend import RedisCache
from snuba.utils.codecs import PassthroughCodec
from snuba.utils.concurrent import execute
from tests.assertions import assert_changes, assert_does_not_change


@pytest.fixture
def backend() -> Iterator[Cache[bytes]]:
    codec: PassthroughCodec[bytes] = PassthroughCodec()
    backend: Cache[bytes] = RedisCache(
        redis_client, "test", codec, ThreadPoolExecutor()
    )
    try:
        yield backend
    finally:
        redis_client.flushdb()


def noop(value: int) -> None:
    return None


def test_get_readthrough(backend: Cache[bytes]) -> None:
    key = "key"
    value = b"value"
    function = mock.MagicMock(return_value=value)

    assert backend.get(key) is None

    with assert_changes(lambda: function.call_count, 0, 1):
        backend.get_readthrough(key, function, noop, 5) == value

    assert backend.get(key) == value

    with assert_does_not_change(lambda: function.call_count, 1):
        backend.get_readthrough(key, function, noop, 5) == value


def test_get_readthrough_missed_deadline(backend: Cache[bytes]) -> None:
    key = "key"
    value = b"value"

    def function() -> bytes:
        time.sleep(1.5)
        return value

    with pytest.raises(TimeoutError):
        backend.get_readthrough(key, function, noop, 1)

    assert backend.get(key) is None


def test_get_readthrough_exception(backend: Cache[bytes]) -> None:
    key = "key"

    class CustomException(Exception):
        pass

    def function() -> bytes:
        raise CustomException("error")

    with pytest.raises(CustomException):
        backend.get_readthrough(key, function, noop, 1)

    assert backend.get(key) is None


def test_get_readthrough_set_wait(backend: Cache[bytes]) -> None:
    key = "key"

    def function() -> bytes:
        time.sleep(1)
        return f"{random.random()}".encode("utf-8")

    def worker() -> bytes:
        return backend.get_readthrough(key, function, noop, 10)

    setter = execute(worker)
    waiter = execute(worker)

    assert setter.result() == waiter.result()


def test_get_readthrough_set_wait_error(backend: Cache[bytes]) -> None:
    key = "key"

    class CustomException(Exception):
        pass

    def function() -> bytes:
        time.sleep(1)
        raise CustomException("error")

    def worker() -> bytes:
        return backend.get_readthrough(key, function, noop, 10)

    setter = execute(worker)
    waiter = execute(worker)

    with pytest.raises(CustomException):
        setter.result()

    with pytest.raises(ExecutionError):
        waiter.result()


def test_get_readthrough_set_wait_timeout(backend: Cache[bytes]) -> None:
    key = "key"
    value = b"value"

    def function() -> bytes:
        time.sleep(2.5)
        return value

    def worker(timeout: int) -> bytes:
        return backend.get_readthrough(key, function, noop, timeout)

    setter = execute(partial(worker, 2))
    waiter_fast = execute(partial(worker, 1))
    waiter_slow = execute(partial(worker, 3))

    with pytest.raises(TimeoutError):
        assert setter.result()

    with pytest.raises(TimeoutError):
        waiter_fast.result()

    with pytest.raises(ExecutionTimeoutError):
        waiter_slow.result()
