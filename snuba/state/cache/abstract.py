from abc import ABC, abstractmethod
from typing import Callable, Generic, Optional, TypeVar


TValue = TypeVar("TValue")


class ExecutionError(Exception):
    pass


class ExecutionTimeoutError(ExecutionError):
    pass


class Cache(Generic[TValue], ABC):
    @abstractmethod
    def get(self, key: str) -> Optional[TValue]:
        """
        Gets a value from the cache.
        """
        raise NotImplementedError

    @abstractmethod
    def set(self, key: str, value: TValue) -> None:
        """
        Sets a value in the cache.
        """
        raise NotImplementedError

    @abstractmethod
    def get_readthrough(
        self, key: str, function: Callable[[], TValue], timeout: int
    ) -> TValue:
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
