import uuid
from abc import ABC, abstractmethod
from typing import Callable, Generic, Optional, TypeVar

from snuba.redis import RedisClientType
from snuba.state import get_config
from snuba.utils.codecs import Codec


T = TypeVar("T")


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


TASK_RESULT = 0
TASK_EXECUTE = 1
TASK_WAIT = 2

TASK_GET = ""
TASK_SET = ""


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

        task_id = uuid.uuid1().hex
        keys = [
            key,
            f"{key}:task",
            f"{key}:task:{task_id}",
        ]
        argv = [uuid.uuid1().hex, timeout]
        result = self.__client.eval(TASK_GET, len(keys), [*keys, *argv])

        if result[0] == TASK_RESULT:
            return self.__codec.decode(result[1])
        elif result[1] == TASK_EXECUTE:
            active_task_id = result[1][0]
            notify_list_key = f"{key}:task:{active_task_id}:notify"
            try:
                success, value = True, function()
            except Exception:
                success = False
            finally:
                keys = [key, f"{key}:task", f"{key}:task:{task_id}", notify_list_key]
                notify_list_timeout = 60
                key_timeout = get_config("cache_expiry_sec", 1)
                argv = [
                    key_timeout,
                    notify_list_timeout,
                ]
                if success:
                    argv.append(self.__codec.encode(value))
                # TODO: Ideally this would let us know if this took effect or not.
                self.__client.eval(TASK_SET, len(keys), [*keys, *argv])
                return value
        elif result[2] == TASK_WAIT:
            active_task_id = result[1][0]
            active_task_timeout_remaining = int(result[1][1])

            notify_list_key = f"{key}:task:{active_task_id}:notify"
            effective_timeout = min(active_task_timeout_remaining, timeout)
            block_result = self.__client.blpop([notify_list_key], effective_timeout)
            if block_result is None:
                if effective_timeout == timeout:
                    raise TimeoutError("client timeout reached")
                else:
                    raise ExecutionTimeoutError("execution timeout reached")
            else:
                assert block_result[0][0] == notify_list_key

            raw_value = self.__client.get(key)
            if raw_value is None:
                raise ExecutionError("cache value not set")
            else:
                return self.__codec.decode(raw_value)
        else:
            raise ValueError("unexpected result from script")
