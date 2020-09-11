from __future__ import annotations

import time
from abc import ABC, abstractmethod
from typing import (
    Callable,
    Generic,
    Iterator as IteratorType,
    Optional,
    MutableSequence,
    Sequence,
    TypeVar,
)


T = TypeVar("T")
R = TypeVar("R")


class Source(ABC, Generic[T]):
    @abstractmethod
    def poll(self, timeout: Optional[float] = None) -> Optional[T]:
        raise NotImplementedError

    def filter(self, function: Callable[[T], bool]) -> Source[T]:
        return Filter(self, function)

    def map(self, function: Callable[[T], R]) -> Source[R]:
        return Map(self, function)

    def batch(self, size: int) -> Source[Sequence[T]]:
        return Batch(self, size)


class Iterator(Source[T]):
    def __init__(self, iterator: IteratorType[T]) -> None:
        self.__iterator = iterator

    def poll(self, timeout: Optional[float] = None) -> Optional[T]:
        return next(self.__iterator)


class Filter(Source[T]):
    def __init__(self, source: Source[T], function: Callable[[T], bool]) -> None:
        self.__source = source
        self.__function = function

    def poll(self, timeout: Optional[float] = None) -> Optional[T]:
        value = self.__source.poll(timeout)

        if value is None:
            return None

        return value if self.__function(value) else None


class Map(Source[R]):
    def __init__(self, source: Source[T], function: Callable[[T], R]) -> None:
        self.__source = source
        self.__function = function

    def poll(self, timeout: Optional[float] = None) -> Optional[R]:
        value = self.__source.poll(timeout)

        if value is None:
            return None

        return self.__function(value)


class Batch(Source[Sequence[T]]):
    def __init__(self, source: Source[T], size: int) -> None:
        self.__source = source
        self.__size = size

        self.__batch: MutableSequence[T] = []

    def poll(self, timeout: Optional[float] = None) -> Optional[Sequence[T]]:
        # TODO: This needs to mock the clock out for testing purposes.
        deadline = time.time() + timeout if timeout is not None else None

        while self.__size > len(self.__batch):
            value = self.__source.poll(
                deadline - time.time() if deadline is not None else None
            )
            if value is not None:
                self.__batch.append(value)

        batch = self.__batch
        self.__batch = []
        return batch
