from __future__ import annotations

import random
import time
from concurrent.futures import Future
from threading import Thread
from typing import Any, Callable
from unittest import mock

import pytest
import rapidjson
from sentry_redis_tools.failover_redis import FailoverRedis

from redis import RedisError
from redis.exceptions import ReadOnlyError
from snuba.redis import RedisClientKey, get_redis_client
from snuba.state import set_config
from snuba.state.cache.abstract import Cache
from snuba.state.cache.redis.backend import RedisCache
from snuba.utils.codecs import ExceptionAwareCodec
from snuba.utils.serializable_exception import (
    SerializableException,
    SerializableExceptionDict,
)
from tests.assertions import assert_changes, assert_does_not_change

redis_client = get_redis_client(RedisClientKey.CACHE)


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


class SingleCallFunction:
    def __init__(self, func: Callable[[], bytes]) -> None:
        self._func = func
        self._count = 0

    def __call__(self, *args: str, **kwargs: str) -> Any:
        self._count += 1
        if self._count > 1:
            raise Exception(
                "This function should only be called once, the cache is doing something wrong"
            )
        return self._func(*args, **kwargs)


class PassthroughCodec(ExceptionAwareCodec[bytes, bytes]):
    def encode(self, value: bytes) -> bytes:
        return value

    def decode(self, value: bytes) -> bytes:
        try:
            ret: SerializableExceptionDict = rapidjson.loads(value)
            if not isinstance(ret, dict):
                return value
            if ret.get("__type__", "NOP") == "SerializableException":
                raise SerializableException.from_dict(ret)
            return value
        except rapidjson.JSONDecodeError:
            pass
        return value

    def encode_exception(self, value: SerializableException) -> bytes:
        return bytes(rapidjson.dumps(value.to_dict()).encode("utf-8"))


@pytest.fixture
def backend() -> Cache[bytes]:
    codec = PassthroughCodec()
    backend: Cache[bytes] = RedisCache(redis_client, "test", codec)
    return backend


@pytest.fixture
def bad_backend() -> Cache[bytes]:
    codec = PassthroughCodec()

    class BadClient(FailoverRedis):
        def __init__(self, client: Any) -> None:
            self._client = client

        def get(self, *args: str, **kwargs: str) -> None:
            raise ReadOnlyError("Failed")

        def __getattr__(self, attr: str) -> Any:
            return getattr(self._client, attr)

    backend: Cache[bytes] = RedisCache(BadClient(redis_client), "test", codec)
    return backend


def noop(value: int) -> None:
    return None


@pytest.mark.redis_db
def test_short_circuit(backend: Cache[bytes]) -> None:
    set_config("read_through_cache.short_circuit", 1)
    key = "key"
    value = b"value"
    function = mock.MagicMock(return_value=value)

    assert backend.get(key) is None

    with assert_changes(lambda: function.call_count, 0, 1):
        backend.get_readthrough(key, function, noop) == value

    assert backend.get(key) is None

    with assert_changes(lambda: function.call_count, 1, 2):
        backend.get_readthrough(key, function, noop) == value


@pytest.mark.redis_db
def test_fail_open(bad_backend: Cache[bytes]) -> None:
    key = "key"
    value = b"value"
    function = mock.MagicMock(return_value=value)
    with mock.patch("snuba.settings.RAISE_ON_READTHROUGH_CACHE_REDIS_FAILURES", False):
        assert bad_backend.get_readthrough(key, function, noop) == value


@pytest.mark.redis_db
def test_get_readthrough(backend: Cache[bytes]) -> None:
    key = "key"
    value = b"value"
    function = mock.MagicMock(return_value=value)

    assert backend.get(key) is None

    with assert_changes(lambda: function.call_count, 0, 1):
        backend.get_readthrough(key, function, noop) == value

    assert backend.get(key) == value

    with assert_does_not_change(lambda: function.call_count, 1):
        backend.get_readthrough(key, function, noop) == value


@pytest.mark.redis_db
def test_get_readthrough_exception(backend: Cache[bytes]) -> None:
    key = "key"

    class CustomException(SerializableException):
        pass

    def function() -> bytes:
        raise CustomException("error")

    with pytest.raises(CustomException):
        backend.get_readthrough(key, SingleCallFunction(function), noop)


@pytest.mark.redis_db
def test_get_readthrough_set_wait(backend: Cache[bytes]) -> None:
    key = "key"

    def function() -> bytes:
        time.sleep(1)
        return f"{random.random()}".encode("utf-8")

    def worker() -> bytes:
        return backend.get_readthrough(key, function, noop)

    setter = worker()
    waiter = worker()

    assert setter == waiter


@pytest.mark.redis_db
def test_get_readthrough_set_wait_error(backend: Cache[bytes]) -> None:
    key = "key"

    class ReadThroughCustomException(SerializableException):
        pass

    def function() -> bytes:
        raise ReadThroughCustomException("error")

    def worker() -> bytes:
        return backend.get_readthrough(key, SingleCallFunction(function), noop)

    setter = execute(worker)
    time.sleep(0.5)
    waiter = execute(worker)

    with pytest.raises(ReadThroughCustomException):
        setter.result()

    # notice that we raised the same exception class in the waiter despite it being deserialized
    # from redis
    with pytest.raises(ReadThroughCustomException) as excinfo:
        waiter.result()

    assert excinfo.value.message == "error"


@pytest.mark.parametrize(
    "backend",
    [
        pytest.param(
            RedisCache(redis_client, "test", PassthroughCodec()),
            id="regular cluster",
        ),
    ],
)
@pytest.mark.redis_db
def test_transient_error(backend: Cache[bytes]) -> None:
    key = "key"

    class SomeTransientException(SerializableException):
        pass

    def error_function() -> bytes:
        raise SomeTransientException("error")

    def normal_function() -> bytes:
        return b"hello"

    def transient_error() -> bytes:
        return backend.get_readthrough(key, SingleCallFunction(error_function), noop)

    def functioning_query() -> bytes:
        return backend.get_readthrough(key, SingleCallFunction(normal_function), noop)

    setter = execute(transient_error)
    # if this sleep were removed, the waiter would also raise
    # SomeTransientException because it would be in the waiting queue and would
    # have the error value propogated to it
    time.sleep(0.01)
    waiter = execute(functioning_query)

    with pytest.raises(SomeTransientException):
        setter.result()

    assert waiter.result() == b"hello"


@pytest.mark.redis_db
def test_set_fails_open(backend: Cache[bytes]) -> None:
    assert isinstance(backend, RedisCache)
    with mock.patch.object(redis_client, "set", side_effect=RedisError()):
        backend.get_readthrough("key", lambda: b"value", noop)
