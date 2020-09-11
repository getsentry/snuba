from __future__ import annotations

import concurrent.futures
import time
from abc import ABC, abstractmethod
from collections import deque
from concurrent.futures import Future
from typing import (
    Callable,
    Deque,
    Generic,
    Iterable,
    Iterator,
    MutableSequence,
    Optional,
    Sequence,
    TypeVar,
    Union,
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
        return FilteredSource(self, function)

    def map(self, target: Union[Callable[[T], R], ParallelExecutor[T, R]]) -> Source[R]:
        if isinstance(target, ParallelExecutor):
            return ParallelMappedSource(self, target)
        else:
            return MappedSource(self, target)

    def batch(self, size: int) -> Source[Sequence[T]]:
        return BatchedSource(self, size)


class IterableSource(Source[T]):
    def __init__(self, iterable: Iterable[T]) -> None:
        self.__iterator = iter(iterable)

    def poll(self, timeout: Optional[float] = None) -> T:
        try:
            return next(self.__iterator)
        except StopIteration:
            raise EndOfStream()


class FilteredSource(Source[T]):
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


class MappedSource(Source[R]):
    def __init__(self, source: Source[T], function: Callable[[T], R]) -> None:
        self.__source = source
        self.__function = function

    def poll(self, timeout: Optional[float] = None) -> R:
        return self.__function(self.__source.poll(timeout))


class Rejected(Exception):
    pass


class ParallelExecutor(ABC, Generic[T, R]):
    @abstractmethod
    def __len__(self) -> int:
        raise NotImplementedError

    @abstractmethod
    def poll(self, timeout: Optional[float] = None) -> R:
        raise NotImplementedError

    @abstractmethod
    def submit(self, value: T) -> None:
        # This method will throw a ``Rejected`` exception if the executor
        # cannot accept the value to be processed at this time.
        raise NotImplementedError

    @abstractmethod
    def close(self) -> None:
        raise NotImplementedError

    @abstractmethod
    def closed(self) -> bool:
        raise NotImplementedError


class ThreadedParallelExecutor(ParallelExecutor[T, R]):
    def __init__(
        self, function: Callable[[T], R], threads: Optional[int] = None
    ) -> None:
        self.__function = function
        self.__executor = concurrent.futures.ThreadPoolExecutor(threads)
        self.__futures: Deque[Future[R]] = deque()
        self.__closed = False

    def __len__(self) -> int:
        return len(self.__futures)

    def poll(self, timeout: Optional[float] = None) -> R:
        try:
            future = self.__futures[0]
        except IndexError as e:
            raise TimeoutError() from e

        try:
            result = future.result(timeout)
        except concurrent.futures.TimeoutError as e:
            raise TimeoutError() from e
        else:
            return result
        finally:
            assert self.__futures.popleft() is future

    def submit(self, value: T) -> None:
        assert not self.__closed
        self.__futures.append(self.__executor.submit(self.__function, value))

    def close(self) -> None:
        self.__closed = True
        self.__executor.shutdown()

    def closed(self) -> bool:
        return self.__closed


class ParallelMappedSource(Source[R]):
    def __init__(self, source: Source[T], executor: ParallelExecutor[T, R]) -> None:
        self.__source = source
        self.__executor = executor

        # XXX: ``Optional`` may be a valid value  of ``T``!
        self.__next_value: Optional[T] = None

    def poll(self, timeout: Optional[float] = None) -> R:
        # Ensure we're always keeping the executor at full capacity as long as
        # there are messages available without blocking and the executor can
        # accept them.
        while True:
            try:
                if self.__next_value is None:
                    self.__next_value = self.__source.poll(0)
            except EndOfStream:
                # If the executor hasn't already been closed, close it now.
                # This ensures all values are actually submitted to the worker
                # pool and we're not stuck waiting on values that are being
                # buffered to be sent as part of a larger batch.
                if not self.__executor.closed():
                    self.__executor.close()

                # If the source has reached the end of the stream but there are
                # still pending values in the executor, we need to keep open
                # until those tasks are completed.
                if not len(self.__executor) > 0:
                    raise
                else:
                    break
            except TimeoutError:
                break

            try:
                self.__executor.submit(self.__next_value)
            except Rejected:
                break

            self.__next_value = None

        # Check if there are any results available to return.
        return self.__executor.poll(timeout)


class BatchedSource(Source[Sequence[T]]):
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
