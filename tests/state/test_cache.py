from __future__ import annotations

import random
import time
from concurrent.futures import Future, ThreadPoolExecutor
from functools import partial
from threading import Thread
from typing import Any, Callable, Iterator, cast
from unittest import mock

import pytest
import rapidjson

from snuba.redis import RedisClientType, redis_client
from snuba.state.cache.abstract import (
    Cache,
    ExecutionError,
    ExecutionTimeoutError,
    TigerExecutionTimeoutError,
)
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
        try:
            ret = rapidjson.loads(value)
            if not isinstance(ret, dict):
                return value
            if ret.get("__type__", "NOP") == "SerializableException":
                raise SerializableException.from_dict(ret)
            return value
        except rapidjson.JSONDecodeError:
            pass
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

    class ReadThroughCustomException(SerializableException):
        pass

    def function() -> bytes:
        time.sleep(1)
        raise ReadThroughCustomException("error")

    def worker() -> bytes:
        return backend.get_readthrough(key, function, noop, 10)

    setter = execute(worker)
    time.sleep(0.5)
    waiter = execute(worker)

    with pytest.raises(ReadThroughCustomException):
        setter.result()

    # pytest assertRaises does not give us the actual exception object
    # so we implement it ourselves as we need it here
    raised_exc = False
    try:
        waiter.result()
    except ReadThroughCustomException as e:
        # notice that we raised the same exception class in the waiter despite it being deserialized
        # from redis
        raised_exc = True
        assert e.message == "error"
    assert raised_exc


@pytest.mark.parametrize(
    "backend",
    [
        pytest.param(
            RedisCache(redis_client, "test", PassthroughCodec(), ThreadPoolExecutor()),
            id="regular cluster",
        ),
        pytest.param(
            RedisCache(
                redis_client,
                "test",
                PassthroughCodec(),
                ThreadPoolExecutor(),
                TigerExecutionTimeoutError,
            ),
            id="tiger cluster",
        ),
    ],
)
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

    with pytest.raises(
        (ExecutionError, ExecutionTimeoutError, TigerExecutionTimeoutError)
    ):
        waiter_slow.result()


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


def test_notify_queue_ttl() -> None:
    # Tests that waiting clients can be notified of the cache status
    # even with network delays. This test will break if the notify queue
    # TTL is set below 200ms

    pop_calls = 0
    num_waiters = 9

    class DelayedRedisClient:
        def __init__(self, redis_client):
            self._client = redis_client

        def __getattr__(self, attr: str):
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
    try:

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
            return delayed_backend.get_readthrough(
                key, normal_function_uncached, noop, 10
            )

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
    finally:
        redis_client.flushdb()
