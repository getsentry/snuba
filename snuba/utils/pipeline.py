from __future__ import annotations

import concurrent.futures
import pickle
import signal
import time
from abc import ABC, abstractmethod
from collections import deque
from concurrent.futures import Future
from multiprocessing import Pool
from multiprocessing.managers import SharedMemoryManager
from multiprocessing.pool import AsyncResult
from multiprocessing.shared_memory import SharedMemory
from pickle import PickleBuffer
from typing import (
    Callable,
    Deque,
    Generic,
    Iterable,
    Iterator,
    MutableSequence,
    Optional,
    Sequence,
    Tuple,
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


# A serialized item is composed of a pickled item value (bytes) and a sequence
# of ``(offset, length)`` that referenced locations in a shared memory block
# for out of band buffer transfer.
SerializedItem = Tuple[bytes, Sequence[Tuple[int, int]]]


class ValueTooLarge(ValueError):
    """
    Raised when a item contains buffers that are too large to be written to
    a shared memory block.
    """


class Batch(Generic[T]):
    """
    Contains a sequence of items that are intended to be shared across
    processes.
    """

    def __init__(self, block: SharedMemory) -> None:
        self.block = block
        self.__items: MutableSequence[SerializedItem] = []
        self.__offset = 0

    def __len__(self) -> int:
        return len(self.__items)

    def __getitem__(self, index: int) -> T:
        """
        Get an item in this batch by its index.

        The item returned by this method is effectively a copy of the
        original item within this batch, and may be safely passed around
        without requiring any special accomodation to keep the shared block
        open or free from conflicting updates.
        """
        data, buffers = self.__items[index]
        # The buffers read from the shared memory block are converted to
        # ``bytes`` rather than being forwarded as ``memoryview`` for two
        # reasons. First, true buffer support protocol is still pretty rare (at
        # writing, it is not supported by either standard library ``json`` or
        # ``rapidjson``, nor by the Confluent Kafka producer), so we'd be
        # copying these values at a later stage anyway. Second, copying these
        # values from the shared memory block (rather than referencing location
        # within it) means that we do not have to ensure that the shared memory
        # block is not recycled during the life of one of these instances. If
        # the shared memory block was reused for a different batch while one of
        # the instances returned by this method was still "alive" in a
        # different part of the processing pipeline, the contents of the
        # message would be liable to be corrupted (at best -- possibly causing
        # a data leak/security issue at worst.)
        return pickle.loads(
            data,
            buffers=[
                self.block.buf[offset : offset + length].tobytes()
                for offset, length in buffers
            ],
        )

    def __iter__(self) -> Iterator[T]:
        """
        Iterate through the items contained in this batch.

        See ``__getitem__`` for more details about the values yielded by the
        iterator returned by this method.
        """
        for i in range(len(self.__items)):
            yield self[i]

    def append(self, item: T) -> None:
        """
        Add an item to this batch.

        Internally, this serializes the message using ``pickle`` (effectively
        creating a copy of the input), writing any data that supports
        out-of-band buffer transfer via the ``PickleBuffer`` interface to the
        shared memory block associated with this batch. If there is not
        enough space in the shared memory block to write all buffers to be
        transferred out-of-band, this method will raise a ``ValueTooLarge``
        error.
        """
        buffers: MutableSequence[Tuple[int, int]] = []

        def buffer_callback(buffer: PickleBuffer) -> None:
            value = buffer.raw()
            offset = self.__offset
            length = len(value)
            if offset + length > self.block.size:
                raise ValueTooLarge(
                    f"value exceeds available space in block, {length} bytes needed but {self.block.size - offset} bytes free"
                )
            self.block.buf[offset : offset + length] = value
            self.__offset += length
            buffers.append((offset, length))

        data = pickle.dumps(item, protocol=5, buffer_callback=buffer_callback)

        self.__items.append((data, buffers))


def parallel_transform_worker_initializer() -> None:
    # Worker process should ignore ``SIGINT`` so that processing is not
    # interrupted by ``KeyboardInterrupt`` during graceful shutdown.
    signal.signal(signal.SIGINT, signal.SIG_IGN)


def parallel_transform_worker_apply(
    function: Callable[[T], R],
    input_batch: Batch[T],
    output_block: SharedMemory,
    start_index: int = 0,
) -> Tuple[int, Batch[R]]:
    output_batch: Batch[R] = Batch(output_block)

    i = start_index
    while i < len(input_batch):
        value = input_batch[i]
        try:
            output_batch.append(function(value))
        except ValueTooLarge:
            # If the output batch cannot accept the transformed value when the
            # batch is empty, we'll never be able to write it and should error
            # instead of retrying. Otherwise, we need to return the values
            # we've already accumulated and continue processing later.
            if len(output_batch) == 0:
                raise
            else:
                break
        else:
            i += 1

    return (i, output_batch)


class BatchingMultiprocessParallelExecutor(ParallelExecutor[T, R]):
    def __init__(
        self,
        function: Callable[[T], R],
        processes: int,
        input_block_size: int,
        output_block_size: int,
        batch_max_values: int,
    ) -> None:
        self.__function = function
        self.__batch_max_values = batch_max_values

        self.__shared_memory_manager = SharedMemoryManager()
        self.__shared_memory_manager.start()

        self.__pool = Pool(processes, initializer=parallel_transform_worker_initializer)

        self.__input_blocks = [
            self.__shared_memory_manager.SharedMemory(input_block_size)
            for _ in range(processes + 1)
        ]

        self.__output_blocks = [
            self.__shared_memory_manager.SharedMemory(output_block_size)
            for _ in range(processes)
        ]

        self.__input_buffer: Batch[T] = Batch(self.__input_blocks.pop())

        # XXX: This is really dumb
        self.__output_buffer: Deque[R] = deque()

        self.__results: Deque[
            Tuple[Batch[T], AsyncResult[Tuple[int, Batch[R]]]]
        ] = deque()

        self.__closed = False

    def __len__(self) -> int:
        # TODO: This is not accurate when dealing with partial batches at the
        # head of the deque!
        return sum(len(input_batch) for input_batch, _ in self.__results) + len(
            self.__output_buffer
        )

    def __submit_batch(self) -> None:
        try:
            output_block = self.__output_blocks.pop()
        except IndexError as e:
            raise Rejected("no available output blocks") from e

        self.__results.append(
            (
                self.__input_buffer,
                self.__pool.apply_async(
                    parallel_transform_worker_apply,
                    (self.__function, self.__input_buffer, output_block),
                ),
            )
        )

        self.__input_buffer = Batch(self.__input_blocks.pop())

    def submit(self, value: T) -> None:
        assert not self.__closed

        if len(self.__input_buffer) >= self.__batch_max_values:
            self.__submit_batch()

        try:
            self.__input_buffer.append(value)
        except ValueTooLarge:
            if len(self.__input_buffer) == 0:
                raise

            self.__submit_batch()

    def poll(self, timeout: Optional[float] = None) -> R:
        if self.__output_buffer:
            return self.__output_buffer.popleft()

        try:
            input_batch, result = self.__results[0]
            index, output_batch = result.get(timeout)
        except IndexError as e:
            raise TimeoutError() from e

        # Copy the contents of the output batch out of shared memory.
        self.__output_buffer.extend(output_batch)

        if index != len(input_batch):
            raise NotImplementedError
        else:
            self.__input_blocks.append(input_batch.block)
            self.__output_blocks.append(output_batch.block)
            del self.__results[0]

        return self.__output_buffer.popleft()

    def close(self) -> None:
        self.__closed = True

        # If there are any values waiting to be submitted, submit them.
        if len(self.__input_buffer):
            try:
                output_block = self.__output_blocks.pop()
            except IndexError:
                # If there no available blocks, just abandon these items.
                pass
            else:
                self.__results.append(
                    (
                        self.__input_buffer,
                        self.__pool.apply_async(
                            parallel_transform_worker_apply,
                            (self.__function, self.__input_buffer, output_block),
                        ),
                    )
                )

        self.__pool.close()

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


if __name__ == "__main__":
    import itertools
    import functools
    import operator

    source = (
        IterableSource(itertools.count())
        .filter(lambda i: i % 2 == 0)
        .map(
            BatchingMultiprocessParallelExecutor(
                functools.partial(operator.mul, 2),
                processes=4,
                input_block_size=4092,
                output_block_size=4092,
                batch_max_values=100,
            )
        )
        .filter(lambda i: i % 10 == 0)
        .map(str)
        .batch(10)
    )
