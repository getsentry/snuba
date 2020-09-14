import logging
import multiprocessing
import pickle
import signal
import time
from collections import deque
from dataclasses import dataclass
from multiprocessing import Pool
from multiprocessing.managers import SharedMemoryManager
from multiprocessing.pool import AsyncResult
from multiprocessing.shared_memory import SharedMemory
from pickle import PickleBuffer
from typing import (
    Callable,
    Deque,
    Generic,
    Iterator,
    Mapping,
    MutableMapping,
    MutableSequence,
    Optional,
    Sequence,
    Tuple,
    TypeVar,
)

from snuba.utils.streams.processing import MessageRejected, ProcessingStrategy
from snuba.utils.streams.types import Message, Partition, TPayload

ProcessingStep = ProcessingStrategy


logger = logging.getLogger(__name__)


class FilterStep(ProcessingStep[TPayload]):
    """
    Determines if a message should be submitted to the next processing step.
    """

    def __init__(
        self,
        function: Callable[[Message[TPayload]], bool],
        next_step: ProcessingStep[TPayload],
    ):
        self.__test_function = function
        self.__next_step = next_step

        self.__closed = False

    def poll(self) -> None:
        self.__next_step.poll()

    def submit(self, message: Message[TPayload]) -> None:
        assert not self.__closed

        if self.__test_function(message):
            self.__next_step.submit(message)

    def close(self) -> None:
        self.__closed = True

    def terminate(self) -> None:
        self.__closed = True

        logger.debug("Terminating %r...", self.__next_step)
        self.__next_step.terminate()

    def join(self, timeout: Optional[float] = None) -> None:
        self.__next_step.close()
        self.__next_step.join(timeout)


TTransformed = TypeVar("TTransformed")


class TransformStep(ProcessingStep[TPayload]):
    """
    Transforms a message and submits the transformed value to the next
    processing step.
    """

    def __init__(
        self,
        function: Callable[[Message[TPayload]], TTransformed],
        next_step: ProcessingStep[TTransformed],
    ) -> None:
        self.__transform_function = function
        self.__next_step = next_step

        self.__closed = False

    def poll(self) -> None:
        self.__next_step.poll()

    def submit(self, message: Message[TPayload]) -> None:
        assert not self.__closed

        self.__next_step.submit(
            Message(
                message.partition,
                message.offset,
                self.__transform_function(message),
                message.timestamp,
            )
        )

    def close(self) -> None:
        self.__closed = True

    def terminate(self) -> None:
        self.__closed = True

        logger.debug("Terminating %r...", self.__next_step)
        self.__next_step.terminate()

    def join(self, timeout: Optional[float] = None) -> None:
        self.__next_step.close()
        self.__next_step.join(timeout)


# A serialized message is composed of a pickled ``Message`` instance (bytes)
# and a sequence of ``(offset, length)`` that referenced locations in a shared
# memory block for out of band buffer transfer.
SerializedMessage = Tuple[bytes, Sequence[Tuple[int, int]]]


class ValueTooLarge(ValueError):
    """
    Raised when a value is too large to be written to a shared memory block.
    """


class MessageBatch(Generic[TPayload]):
    """
    Contains a sequence of ``Message`` instances that are intended to be
    shared across processes.
    """

    def __init__(self, block: SharedMemory) -> None:
        self.block = block
        self.__items: MutableSequence[SerializedMessage] = []
        self.__offset = 0

    def __repr__(self) -> str:
        return f"<{type(self).__name__}: {len(self)} items, {self.__offset} bytes>"

    def __len__(self) -> int:
        return len(self.__items)

    def __getitem__(self, index: int) -> Message[TPayload]:
        """
        Get a message in this batch by its index.

        The message returned by this method is effectively a copy of the
        original message within this batch, and may be safely passed
        around without requiring any special accomodation to keep the shared
        block open or free from conflicting updates.
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
        # block is not recycled during the life of one of these ``Message``
        # instances. If the shared memory block was reused for a different
        # batch while one of the ``Message`` instances returned by this method
        # was still "alive" in a different part of the processing pipeline, the
        # contents of the message would be liable to be corrupted (at best --
        # possibly causing a data leak/security issue at worst.)
        return pickle.loads(
            data,
            buffers=[
                self.block.buf[offset : offset + length].tobytes()
                for offset, length in buffers
            ],
        )

    def __iter__(self) -> Iterator[Message[TPayload]]:
        """
        Iterate through the messages contained in this batch.

        See ``__getitem__`` for more details about the ``Message`` instances
        yielded by the iterator returned by this method.
        """
        for i in range(len(self.__items)):
            yield self[i]

    def append(self, message: Message[TPayload]) -> None:
        """
        Add a message to this batch.

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

        data = pickle.dumps(message, protocol=5, buffer_callback=buffer_callback)

        self.__items.append((data, buffers))


class BatchBuilder(Generic[TPayload]):
    def __init__(
        self, batch: MessageBatch[TPayload], max_batch_size: int, max_batch_time: float
    ) -> None:
        self.__batch = batch
        self.__max_batch_size = max_batch_size
        self.__deadline = time.time() + max_batch_time

    def __len__(self) -> int:
        return len(self.__batch)

    def append(self, message: Message[TPayload]) -> None:
        self.__batch.append(message)

    def ready(self) -> bool:
        if len(self.__batch) >= self.__max_batch_size:
            return True
        elif time.time() >= self.__deadline:
            return True
        else:
            return False

    def build(self) -> MessageBatch[TPayload]:
        return self.__batch


def parallel_transform_worker_initializer() -> None:
    # Worker process should ignore ``SIGINT`` so that processing is not
    # interrupted by ``KeyboardInterrupt`` during graceful shutdown.
    signal.signal(signal.SIGINT, signal.SIG_IGN)


def parallel_transform_worker_apply(
    function: Callable[[Message[TPayload]], TTransformed],
    input_batch: MessageBatch[TPayload],
    output_block: SharedMemory,
    start_index: int = 0,
) -> Tuple[int, MessageBatch[TTransformed]]:
    output_batch: MessageBatch[TTransformed] = MessageBatch(output_block)

    i = start_index
    while i < len(input_batch):
        message = input_batch[i]
        try:
            output_batch.append(
                Message(
                    message.partition,
                    message.offset,
                    function(message),
                    message.timestamp,
                )
            )
        except ValueTooLarge:
            # If the output batch cannot accept the transformed message when
            # the batch is empty, we'll never be able to write it and should
            # error instead of retrying. Otherwise, we need to return the
            # values we've already accumulated and continue processing later.
            if len(output_batch) == 0:
                raise
            else:
                break
        else:
            i += 1

    return (i, output_batch)


class ParallelTransformStep(ProcessingStep[TPayload]):
    def __init__(
        self,
        function: Callable[[Message[TPayload]], TTransformed],
        next_step: ProcessingStep[TTransformed],
        processes: int,
        max_batch_size: int,
        max_batch_time: float,
        input_block_size: int,
        output_block_size: int,
    ) -> None:
        self.__transform_function = function
        self.__next_step = next_step
        self.__max_batch_size = max_batch_size
        self.__max_batch_time = max_batch_time

        self.__shared_memory_manager = SharedMemoryManager()
        self.__shared_memory_manager.start()

        self.__pool = Pool(processes, initializer=parallel_transform_worker_initializer)

        self.__input_blocks = [
            self.__shared_memory_manager.SharedMemory(input_block_size)
            for _ in range(processes)
        ]

        self.__output_blocks = [
            self.__shared_memory_manager.SharedMemory(output_block_size)
            for _ in range(processes)
        ]

        self.__batch_builder: Optional[BatchBuilder[TPayload]] = None

        self.__results: Deque[
            Tuple[
                MessageBatch[TPayload],
                AsyncResult[Tuple[int, MessageBatch[TTransformed]]],
            ]
        ] = deque()

        self.__closed = False

    def __submit_batch(self) -> None:
        assert self.__batch_builder is not None
        batch = self.__batch_builder.build()
        logger.debug("Submitting %r to %r...", batch, self.__pool)
        self.__results.append(
            (
                batch,
                self.__pool.apply_async(
                    parallel_transform_worker_apply,
                    (self.__transform_function, batch, self.__output_blocks.pop()),
                ),
            )
        )
        self.__batch_builder = None

    def __check_for_results(self, timeout: Optional[float] = None) -> None:
        input_batch, result = self.__results[0]

        i, output_batch = result.get(timeout=timeout)

        # TODO: This does not handle rejections from the next step!
        for message in output_batch:
            self.__next_step.poll()
            self.__next_step.submit(message)

        if i != len(input_batch):
            logger.warning(
                "Received incomplete batch (%0.2f%% complete), resubmitting...",
                i / len(input_batch) * 100,
            )
            # TODO: This reserializes all the ``SerializedMessage`` data prior
            # to the processed index even though the values at those indices
            # will never be unpacked. It probably makes sense to remove that
            # data from the batch to avoid unnecessary serialization overhead.
            self.__results[0] = (
                input_batch,
                self.__pool.apply_async(
                    parallel_transform_worker_apply,
                    (self.__transform_function, input_batch, output_batch.block, i,),
                ),
            )
            return

        logger.debug("Completed %r, reclaiming blocks...", input_batch)
        self.__input_blocks.append(input_batch.block)
        self.__output_blocks.append(output_batch.block)

        del self.__results[0]

    def poll(self) -> None:
        self.__next_step.poll()

        while self.__results:
            try:
                self.__check_for_results(timeout=0)
            except multiprocessing.TimeoutError:
                break

        if self.__batch_builder is not None and self.__batch_builder.ready():
            self.__submit_batch()

    def __reset_batch_builder(self) -> None:
        try:
            input_block = self.__input_blocks.pop()
        except IndexError as e:
            raise MessageRejected("no available input blocks") from e

        self.__batch_builder = BatchBuilder(
            MessageBatch(input_block), self.__max_batch_size, self.__max_batch_time,
        )

    def submit(self, message: Message[TPayload]) -> None:
        assert not self.__closed

        if self.__batch_builder is None:
            self.__reset_batch_builder()
            assert self.__batch_builder is not None

        try:
            self.__batch_builder.append(message)
        except ValueTooLarge as e:
            logger.debug("Caught %r, closing batch and retrying...", e)
            self.__submit_batch()

            # This may raise ``MessageRejected`` (if all of the shared memory
            # is in use) and create backpressure.
            self.__reset_batch_builder()
            assert self.__batch_builder is not None

            # If this raises ``ValueTooLarge``, that means that the input block
            # size is too small (smaller than the Kafka payload limit without
            # compression.)
            self.__batch_builder.append(message)

    def close(self) -> None:
        self.__closed = True

        if self.__batch_builder is not None and len(self.__batch_builder) > 0:
            self.__submit_batch()

    def terminate(self) -> None:
        self.__closed = True

        logger.debug("Terminating %r...", self.__pool)
        self.__pool.terminate()

        logger.debug("Shutting down %r...", self.__shared_memory_manager)
        self.__shared_memory_manager.shutdown()

        logger.debug("Terminating %r...", self.__next_step)
        self.__next_step.terminate()

    def join(self, timeout: Optional[float] = None) -> None:
        deadline = time.time() + timeout if timeout is not None else None

        logger.debug("Waiting for %s batches...", len(self.__results))
        while self.__results:
            self.__check_for_results(
                timeout=max(deadline - time.time(), 0) if deadline is not None else None
            )

        self.__pool.close()

        logger.debug("Waiting for %s...", self.__pool)
        # ``Pool.join`` doesn't accept a timeout (?!) but this really shouldn't
        # block for any significant amount of time unless something really went
        # wrong (i.e. we lost track of a task)
        self.__pool.join()

        self.__shared_memory_manager.shutdown()

        self.__next_step.close()
        self.__next_step.join(
            timeout=max(deadline - time.time(), 0) if deadline is not None else None
        )


@dataclass
class OffsetRange:
    __slots__ = ["lo", "hi"]

    lo: int  # inclusive
    hi: int  # exclusive


class Batch(Generic[TPayload]):
    def __init__(
        self,
        step: ProcessingStep[TPayload],
        commit_function: Callable[[Mapping[Partition, int]], None],
    ) -> None:
        self.__step = step
        self.__commit_function = commit_function

        self.__created = time.time()
        self.__length = 0
        self.__offsets: MutableMapping[Partition, OffsetRange] = {}
        self.__closed = False

    def __repr__(self) -> str:
        return f"<{type(self).__name__}: {len(self)} message{'s' if len(self) != 1 else ''}, open for {self.duration():0.2f} seconds>"

    def __len__(self) -> int:
        return self.__length

    def duration(self) -> float:
        return time.time() - self.__created

    def poll(self) -> None:
        self.__step.poll()

    def submit(self, message: Message[TPayload]) -> None:
        assert not self.__closed

        self.__step.submit(message)
        self.__length += 1

        if message.partition in self.__offsets:
            self.__offsets[message.partition].hi = message.next_offset
        else:
            self.__offsets[message.partition] = OffsetRange(
                message.offset, message.next_offset
            )

    def close(self) -> None:
        self.__closed = True
        self.__step.close()

    def terminate(self) -> None:
        self.__closed = True

        logger.debug("Terminating %r...", self.__step)
        self.__step.terminate()

    def join(self, timeout: Optional[float] = None) -> None:
        self.__step.join(timeout)
        offsets = {
            partition: offsets.hi for partition, offsets in self.__offsets.items()
        }
        logger.debug("Committing offsets: %r", offsets)
        self.__commit_function(offsets)


class CollectStep(ProcessingStep[TPayload]):
    """
    Collects messages into batches, periodically closing the batch and
    committing the offsets once the batch has successfully been closed.
    """

    def __init__(
        self,
        step_factory: Callable[[], ProcessingStep[TPayload]],
        commit_function: Callable[[Mapping[Partition, int]], None],
        max_batch_size: int,
        max_batch_time: float,
    ) -> None:
        self.__step_factory = step_factory
        self.__commit_function = commit_function
        self.__max_batch_size = max_batch_size
        self.__max_batch_time = max_batch_time

        self.__batch: Optional[Batch[TPayload]] = None
        self.__closed = False

    def __close_and_reset_batch(self) -> None:
        assert self.__batch is not None
        self.__batch.close()
        self.__batch.join()
        logger.info("Completed processing %r.", self.__batch)
        self.__batch = None

    def poll(self) -> None:
        if self.__batch is None:
            return

        self.__batch.poll()

        # XXX: This adds a substantially blocking operation to the ``poll``
        # method which is bad.
        if len(self.__batch) >= self.__max_batch_size:
            logger.debug("Size limit reached, closing %r...", self.__batch)
            self.__close_and_reset_batch()
        elif self.__batch.duration() >= self.__max_batch_time:
            logger.debug("Time limit reached, closing %r...", self.__batch)
            self.__close_and_reset_batch()

    def submit(self, message: Message[TPayload]) -> None:
        assert not self.__closed

        if self.__batch is None:
            self.__batch = Batch(self.__step_factory(), self.__commit_function)

        self.__batch.submit(message)

    def close(self) -> None:
        self.__closed = True

        if self.__batch is not None:
            logger.debug("Closing %r...", self.__batch)
            self.__batch.close()

    def terminate(self) -> None:
        self.__closed = True

        if self.__batch is not None:
            self.__batch.terminate()

    def join(self, timeout: Optional[float] = None) -> None:
        if self.__batch is not None:
            self.__batch.join(timeout)
            logger.info("Completed processing %r.", self.__batch)
            self.__batch = None
