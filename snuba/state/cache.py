from abc import ABC, abstractmethod
from typing import Generic, Optional, TypeVar

from snuba.redis import RedisClientType
from snuba.state import get_config
from snuba.utils.codecs import Codec


T = TypeVar("T")


class Cache(Generic[T], ABC):
    @abstractmethod
    def get(self, key: str) -> Optional[T]:
        raise NotImplementedError

    @abstractmethod
    def set(self, key: str, value: T) -> None:
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
