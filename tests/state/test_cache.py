from __future__ import annotations

import random
import time
from concurrent.futures import Future, ThreadPoolExecutor
from functools import partial
from threading import Thread
from typing import Any, Callable, Iterator
from unittest import mock

import pytest
import rapidjson

from snuba.redis import redis_client
from snuba.state.cache.abstract import Cache, ExecutionTimeoutError
from snuba.state.cache.redis.backend import RedisCache
from snuba.utils.codecs import ExceptionAwareCodec
from snuba.utils.serializable_exception import SerializableException
from tests.assertions import assert_changes, assert_does_not_change


def execute(function: Callable[[], Any]) -> Future[Any]:
    future: Future[Any] = Future()

    def run() -> None:
        try:
            result = function()
        except Exception as e:
            future.set_exception(e)
        else:
            future.set_result(result)

    Thread(target=run).start()

    return future


class PassthroughCodec(ExceptionAwareCodec[bytes, bytes]):
    def encode(self, value: bytes) -> bytes:
        return value

    def decode(self, value: bytes) -> bytes:
        return value

    def encode_exception(self, value: SerializableException) -> bytes:
        return rapidjson.dumps(value.to_dict()).encode("utf-8")


@pytest.fixture
def backend() -> Iterator[Cache[bytes]]:
    codec = PassthroughCodec()
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

    class CustomException(SerializableException):
        pass

    def function() -> bytes:
        raise CustomException("error")

    with pytest.raises(CustomException):
        backend.get_readthrough(key, function, noop, 1)


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

    class CustomException(SerializableException):
        pass

    def function() -> bytes:
        time.sleep(1)
        raise CustomException("error")

    def worker() -> bytes:
        return backend.get_readthrough(key, function, noop, 10)

    setter = execute(worker)
    time.sleep(0.5)
    waiter = execute(worker)

    with pytest.raises(CustomException):
        setter.result()

    with pytest.raises(Exception) as e:
        waiter.result()
        assert e.__class__.__name__ == "CustomException"
        assert e.args == ("error",)


def test_get_readthrough_set_wait_timeout(backend: Cache[bytes]) -> None:
    key = "key"
    value = b"value"

    def function(id: int) -> bytes:
        time.sleep(2.5)
        return value + f"{id}".encode()

    def worker(timeout: int) -> bytes:
        return backend.get_readthrough(key, partial(function, timeout), noop, timeout)

    setter = execute(partial(worker, 2))
    time.sleep(0.1)
    waiter_fast = execute(partial(worker, 1))
    time.sleep(0.1)
    waiter_slow = execute(partial(worker, 3))

    with pytest.raises(TimeoutError):
        assert setter.result()

    with pytest.raises(TimeoutError):
        waiter_fast.result()

    with pytest.raises(ExecutionTimeoutError):
        waiter_slow.result()
