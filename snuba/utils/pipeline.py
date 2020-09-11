from __future__ import annotations

import time
from abc import ABC, abstractmethod
from typing import (
    Callable,
    Generic,
    Iterable,
    Iterator,
    MutableSequence,
    Optional,
    Sequence,
    TypeVar,
)

T = TypeVar("T")
R = TypeVar("R")


class EndOfStream(StopIteration):
    """
    Exception raised when the end of a stream has been reached.
    """


class Source(ABC, Generic[T]):
    @abstractmethod
    def poll(self, timeout: Optional[float] = None) -> T:
        """
        Poll the source for the next value.

        This method will throw a ``TimeoutError`` if a value is not available
        within the given timeout.
        """
        raise NotImplementedError

    def __iter__(self) -> Iterator[T]:
        try:
            while True:
                yield self.poll()
        except EndOfStream:
            return

    def filter(self, function: Callable[[T], bool]) -> Source[T]:
        return Filter(self, function)

    def map(self, function: Callable[[T], R]) -> Source[R]:
        return Map(self, function)

    def batch(self, size: int) -> Source[Sequence[T]]:
        return Batch(self, size)


class IterableSource(Source[T]):
    def __init__(self, iterable: Iterable[T]) -> None:
        self.__iterator = iter(iterable)

    def poll(self, timeout: Optional[float] = None) -> T:
        try:
            return next(self.__iterator)
        except StopIteration:
            raise EndOfStream()


class Filter(Source[T]):
    def __init__(self, source: Source[T], function: Callable[[T], bool]) -> None:
        self.__source = source
        self.__function = function

    def poll(self, timeout: Optional[float] = None) -> T:
        # TODO: This needs to mock the clock out for testing purposes.
        deadline = time.time() + timeout if timeout is not None else None

        while True:
            value = self.__source.poll(
                deadline - time.time() if deadline is not None else None
            )
            if self.__function(value):
                return value


class Map(Source[R]):
    def __init__(self, source: Source[T], function: Callable[[T], R]) -> None:
        self.__source = source
        self.__function = function

    def poll(self, timeout: Optional[float] = None) -> R:
        return self.__function(self.__source.poll(timeout))


class Batch(Source[Sequence[T]]):
    def __init__(self, source: Source[T], size: int) -> None:
        self.__source = source
        self.__size = size

        self.__batch: MutableSequence[T] = []

    def poll(self, timeout: Optional[float] = None) -> Sequence[T]:
        # TODO: This needs to mock the clock out for testing purposes.
        deadline = time.time() + timeout if timeout is not None else None

        try:
            while self.__size > len(self.__batch):
                self.__batch.append(
                    self.__source.poll(
                        deadline - time.time() if deadline is not None else None
                    )
                )
        except EndOfStream:
            # If we've reached the end of the stream and there is a batch in
            # progress, suppress the exception so that we can return the batch.
            if not self.__batch:
                raise

        batch = self.__batch
        self.__batch = []
        return batch
