from abc import ABC, abstractmethod
from typing import Callable, Generic, Optional, TypeVar

from snuba.redis import RedisClientType
from snuba.state import get_config
from snuba.utils.codecs import Codec


T = TypeVar("T")


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
    def get_or_execute(self, key: str, function: Callable[[], T], timeout: float) -> T:
        """
        Attempts to get a value from the cache. On a cache miss, the return
        value of the provided function is used to populate the cache for
        subsequent callers and is used as the return value for this method.
        (If the function throws an exception, the cache remains unpopulated
        and that exception will be propagated upwards from this method.)

        This function also acts as an exclusive lock on the cache key while
        the function is executing. Callers will be blocked until the client
        that holds the lock (the first client to get a cache miss) has
        completed executing the function and placed its result in cache.

        If the client holding the lock does not finish executing the function
        prior to the timeout being reached, all blocked clients will raise a
        ``TimeoutError``. Since the timeout clock starts when the first
        client takes the execution lock and begins to execute the function,
        the timeout is an upper bound on the actual amount of time that any
        subsequent clients will be blocked -- they will likely get a result
        or throw a ``TimeoutError`` in a time window substantially shorter
        than the full timeout duration.
        """
        raise NotImplementedError


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
