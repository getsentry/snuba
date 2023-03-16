import time
from typing import Any, Set


class ReplicaExpiringSet:
    def __init__(self, ttl: int) -> None:
        self.__cache: Set[Any] = set()
        self.__ttl = ttl
        self.__start = time.time()
        self._invalidate = False
        self._reset()

    def reset(self) -> None:
        self._invalidate = False
        self._reset()

    def invalidate(self) -> None:
        self._invalidate = True

    def expired(self) -> bool:
        return time.time() - self.__start > self.__ttl

    def isvalid(self) -> bool:
        return not self.expired() and not self._invalidate

    def _check_expired(self) -> bool:
        if self.expired():
            self._reset()
            return True
        return False

    def _reset(self) -> None:
        self.__cache.clear()
        self.__start = time.time()

    def add(self, replica: str) -> None:
        self.__cache.add(replica)

    def __contains__(self, replica: str) -> bool:
        if self._check_expired():
            return False
        return replica in self.__cache
