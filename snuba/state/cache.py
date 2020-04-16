import logging
import uuid
from abc import ABC, abstractmethod
from typing import Callable, Generic, Optional, TypeVar

from pkg_resources import resource_string

from snuba.redis import RedisClientType
from snuba.state import get_config
from snuba.utils.codecs import Codec


T = TypeVar("T")


logger = logging.getLogger(__name__)


class ExecutionError(Exception):
    pass


class ExecutionTimeoutError(ExecutionError):
    pass


class Cache(Generic[T], ABC):
    @abstractmethod
    def get(self, key: str) -> Optional[T]:
        """
        Gets a value from the cache.
        """
        raise NotImplementedError

    @abstractmethod
    def set(self, key: str, value: T) -> None:
        """
        Sets a value in the cache.
        """
        raise NotImplementedError

    @abstractmethod
    def get_or_execute(self, key: str, function: Callable[[], T], timeout: int) -> T:
        """
        Attempts to get a value from the cache. On a cache miss, the return
        value of the provided function is used to populate the cache for
        subsequent callers and is used as the return value for this method.

        This function also acts as an exclusive lock on the cache key while
        the function is executing. Callers will be blocked until the client
        that holds the lock (the first client to get a cache miss) has
        completed executing the function and placed its result in cache.

        If the client holding the lock does not successfully execute the
        function, no result value will be populated. The client that held the
        lock will raise the original exception, and all other clients will raise
        an ``ExecutionError`` when unblocked.

        If the client holding the lock does not finish executing the function
        prior to the timeout being reached, the client that held the lock
        will return the result of executing the provided function, and will
        only set the cache value if none currently exists. Meanwhile, all
        blocked clients will raise a ``ExecutionTimeoutError`` when the
        timeout elapses. Since the timeout clock starts when the first client
        takes the execution lock and begins to execute the function, the
        timeout is an upper bound on the actual amount of time that any
        subsequent clients will be blocked -- they will likely get a result
        or throw a ``ExecutionTimeoutError`` in a time window substantially
        shorter than the full timeout duration.
        """
        raise NotImplementedError


RESULT_VALUE = 0
RESULT_EXECUTE = 1
RESULT_WAIT = 2

SCRIPT_GET = resource_string("snuba", "state/scripts/get.lua")
SCRIPT_SET = resource_string("snuba", "state/scripts/set.lua")


class RedisCache(Cache[T]):
    def __init__(
        self, client: RedisClientType, prefix: str, codec: Codec[str, T]
    ) -> None:
        self.__client = client
        self.__prefix = prefix
        self.__codec = codec

    def __build_key(self, key: str) -> str:
        return f"{self.__prefix}{key}"

    def get(self, key: str) -> Optional[T]:
        value = self.__client.get(self.__build_key(key))
        if value is None:
            return None

        return self.__codec.decode(value)

    def set(self, key: str, value: T) -> None:
        self.__client.set(
            self.__build_key(key),
            self.__codec.encode(value),
            ex=get_config("cache_expiry_sec", 1),
        )

    def get_or_execute(self, key: str, function: Callable[[], T], timeout: int) -> T:
        key = self.__build_key(key)

        keys = [key, f"{key}:q", f"{key}:u"]
        argv = [timeout, uuid.uuid1().hex]
        result = self.__client.eval(SCRIPT_GET, len(keys), *keys, *argv)

        if result[0] == RESULT_VALUE:
            logger.debug("Immediately returning result from cache hit.")
            return self.__codec.decode(result[1])
        elif result[0] == RESULT_EXECUTE:
            task_id = result[1].decode("utf-8")
            logger.debug("Executing task (id: %r)...", task_id)
            keys = [key, f"{key}:q", f"{key}:u", f"{key}:n:{task_id}"]
            argv = [task_id, 60]
            try:
                value = function()
            except Exception:
                raise
            else:
                argv.extend(
                    [self.__codec.encode(value), get_config("cache_expiry_sec", 1)]
                )
            finally:
                logger.debug("Setting result and waking blocked clients...")
                self.__client.eval(SCRIPT_SET, len(keys), *keys, *argv)
            return value
        elif result[0] == RESULT_WAIT:
            task_id = result[1].decode("utf-8")
            timeout_remaining = result[2]
            effective_timeout = min(timeout_remaining, timeout)
            logger.debug(
                "Waiting for task result (id: %r) for up to %s seconds...",
                task_id,
                effective_timeout,
            )
            if not self.__client.blpop(f"{key}:n:{task_id}", effective_timeout):
                raise ExecutionTimeoutError("timed out waiting for result")
            else:
                value = self.__client.get(key)
                if value is None:
                    raise ExecutionError("no value at key")
                else:
                    return self.__codec.decode(value)
        else:
            raise ValueError("unexpected result from script")
