from abc import ABC, abstractmethod
from typing import Callable, Generic, Optional, TypeVar

from snuba.utils.metrics.timer import Timer
from snuba.utils.serializable_exception import SerializableException

TValue = TypeVar("TValue")


class ExecutionError(SerializableException):
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
    def get_cached_result_and_record_timer(
        self,
        key: str,
        timer: Optional[Timer] = None,
    ) -> Optional[TValue]:
        """
        Gets a value from the cache and records timer metric.
        """
        raise NotImplementedError

    @abstractmethod
    def get_readthrough(
        self,
        key: str,
        function: Callable[[], TValue],
        record_cache_hit_type: Callable[[int], None],
        timeout: int,
        timer: Optional[Timer] = None,
    ) -> TValue:
        """
        Implements a read-through caching pattern for the value at the given
        key.

        This method should only be used if `self.get` or
        `self.get_cached_result_and_record_metrics` results in a cache miss.

        On a cache miss, the return value of the provided function is used
        to populate the cache for subsequent callers and is used as the return
        value for this method.

        This function also acts as an exclusive lock on the cache key to
        other callers of this method while the function is executing. Callers
        will be blocked until the client that holds the lock (the first
        client to get a cache miss) has completed executing the function and
        placed its result in cache.

        If the client holding the lock does not successfully execute the
        function, no result value will be populated. The client that held the
        lock will raise the original exception, and all other clients will raise
        an ``ExecutionError`` when unblocked.

        If the client holding the lock does not finish executing the function
        prior to the timeout being reached, the client that held the lock
        will raise a ``TimeoutError`` and will not populate the cache with
        the value upon function completetion to avoid potentially overwriting
        a fresher value with a stale value. Meanwhile, all blocked clients
        will raise a ``ExecutionTimeoutError`` when the timeout elapses.
        Since the timeout clock starts when the first client takes the
        execution lock and begins to execute the function, the timeout is an
        upper bound on the actual amount of time that any subsequent clients
        will be blocked -- they will likely get a result or throw a
        ``ExecutionTimeoutError`` in a time window substantially shorter than
        the full timeout duration.
        """
        raise NotImplementedError
