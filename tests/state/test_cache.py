from __future__ import annotations

import random
import time
from concurrent.futures import Future, ThreadPoolExecutor
from functools import partial
from threading import Thread
from typing import Any, Callable, cast
from unittest import mock

import pytest
import rapidjson

from snuba.redis import FailoverRedis, RedisClientKey, RedisClientType, get_redis_client
from snuba.state import set_config
from snuba.state.cache.abstract import Cache, ExecutionError, ExecutionTimeoutError
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
    backend: Cache[bytes] = RedisCache(
        redis_client, "test", codec, ThreadPoolExecutor()
    )
    return backend


@pytest.fixture
def bad_backend() -> Cache[bytes]:
    codec = PassthroughCodec()

    class BadClient(FailoverRedis):
        def __init__(self, client: Any):
            self._client = client

        def _fail_everything(self, *args, **kwargs):
            raise Exception("Failed")

        def __getattr__(self, attr: str):
            if callable(getattr(self._client, attr, None)):
                return self._fail_everything

    backend: Cache[bytes] = RedisCache(
        BadClient(redis_client), "test", codec, ThreadPoolExecutor()
    )
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
        backend.get_readthrough(key, function, noop, 5) == value

    assert backend.get(key) is None

    with assert_changes(lambda: function.call_count, 1, 2):
        backend.get_readthrough(key, function, noop, 5) == value


@pytest.mark.redis_db
def test_fail_open(bad_backend: Cache[bytes]):
    key = "key"
    value = b"value"
    function = mock.MagicMock(return_value=value)
    with mock.patch("snuba.settings.RAISE_ON_READTHROUGH_CACHE_FAILURES", False):
        assert bad_backend.get_readthrough(key, function, noop, 5) == value


@pytest.mark.redis_db
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


@pytest.mark.redis_db
def test_get_readthrough_missed_deadline(backend: Cache[bytes]) -> None:
    key = "key"
    value = b"value"

    def function() -> bytes:
        time.sleep(1.5)
        return value

    with pytest.raises(TimeoutError):
        backend.get_readthrough(key, function, noop, 1)

    assert backend.get(key) is None


@pytest.mark.redis_db
def test_get_readthrough_exception(backend: Cache[bytes]) -> None:
    key = "key"
    count = 0

    class CustomException(SerializableException):
        pass

    def function() -> bytes:
        nonlocal count
        count += 1
        if count == 1:
            raise CustomException("error")
        return b"should not be called twice"

    with pytest.raises(CustomException):
        backend.get_readthrough(key, function, noop, 1)


@pytest.mark.redis_db
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


@pytest.mark.redis_db
def test_get_readthrough_set_wait_error(backend: Cache[bytes]) -> None:
    key = "key"
    count = 0

    class ReadThroughCustomException(SerializableException):
        pass

    def function() -> bytes:
        time.sleep(1)
        nonlocal count
        count += 1
        if count == 1:
            raise ReadThroughCustomException("error")
        return b"should not be called twice"

    def worker() -> bytes:
        return backend.get_readthrough(key, function, noop, 10)

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
            RedisCache(redis_client, "test", PassthroughCodec(), ThreadPoolExecutor()),
            id="regular cluster",
        ),
    ],
)
@pytest.mark.redis_db
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

    with pytest.raises((ExecutionError, ExecutionTimeoutError)):
        waiter_slow.result()


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
        return backend.get_readthrough(key, error_function, noop, 10)

    def functioning_query() -> bytes:
        return backend.get_readthrough(key, normal_function, noop, 10)

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
def test_notify_queue_ttl() -> None:
    # Tests that waiting clients can be notified of the cache status
    # even with network delays. This test will break if the notify queue
    # TTL is set below 200ms

    pop_calls = 0
    num_waiters = 9

    class DelayedRedisClient:
        def __init__(self, redis_client: RedisClientType) -> None:
            self._client = redis_client

        def __getattr__(self, attr: str) -> Any:
            # simulate the queue pop taking longer than expected.
            # the notification queue TTL is 60 seconds so running into a timeout
            # shouldn't happen (unless something has drastically changed in the TTL
            # time or use)
            if attr == "blpop":
                nonlocal pop_calls
                pop_calls += 1
                time.sleep(0.5)
            return getattr(self._client, attr)

    codec = PassthroughCodec()

    delayed_backend: Cache[bytes] = RedisCache(
        cast(RedisClientType, DelayedRedisClient(redis_client)),
        "test",
        codec,
        ThreadPoolExecutor(),
    )
    key = "key"

    def normal_function() -> bytes:
        # this sleep makes sure that all waiting clients
        # are put into the waiting queue
        time.sleep(0.5)
        return b"hello-cached"

    def normal_function_uncached() -> bytes:
        return b"hello-not-cached"

    def cached_query() -> bytes:
        return delayed_backend.get_readthrough(key, normal_function, noop, 10)

    def uncached_query() -> bytes:
        return delayed_backend.get_readthrough(key, normal_function_uncached, noop, 10)

    setter = execute(cached_query)
    waiters = []
    time.sleep(0.1)
    for _ in range(num_waiters):
        waiters.append(execute(uncached_query))

    # make sure that all clients actually did hit the cache
    assert setter.result() == b"hello-cached"
    for w in waiters:
        assert w.result() == b"hello-cached"
    # make sure that all the waiters actually did hit the notification queue
    # and didn't just get a direct cache hit
    assert pop_calls == num_waiters
