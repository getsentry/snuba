import time
from typing import Any, Set


class ReplicaExpiringSet:
    def __init__(self, ttl: int) -> None:
        self.cache: Set[Any] = set()
        self.is_valid = True
        self.__ttl = ttl
        self.__start = time.time()

    def expired(self) -> bool:
        return time.time() - self.__start > self.__ttl

    def reset(self) -> None:
        self.__start = time.time()
        self.is_valid = True
        self.cache.clear()

    def invalidate(self) -> None:
        self.is_valid = False
