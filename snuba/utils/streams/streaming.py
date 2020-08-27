import logging
import signal
import time
from dataclasses import dataclass
from multiprocessing import Pool
from typing import Callable, Generic, Mapping, MutableMapping, Optional, TypeVar

from snuba.utils.streams.processing import ProcessingStrategy
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

    def join(self, timeout: Optional[float] = None) -> None:
        self.__next_step.close()
        self.__next_step.join(timeout)


def parallel_transform_worker_initializer() -> None:
    signal.signal(signal.SIGINT, signal.SIG_IGN)


class ParallelTransformStep(ProcessingStep[TPayload]):
    def __init__(
        self,
        function: Callable[[Message[TPayload]], TTransformed],
        next_step: ProcessingStep[TTransformed],
        processes: int,
    ) -> None:
        self.__transform_function = function
        self.__next_step = next_step

        self.__pool = Pool(processes, initializer=parallel_transform_worker_initializer)

        self.__closed = False

    def poll(self) -> None:
        raise NotImplementedError

    def submit(self, message: Message[TPayload]) -> None:
        assert not self.__closed

        raise NotImplementedError

    def close(self) -> None:
        self.__closed = True

        self.__pool.close()

    def join(self, timeout: Optional[float] = None) -> None:
        self.__pool.join()


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
            self.__offsets[message.partition].hi = message.get_next_offset()
        else:
            self.__offsets[message.partition] = OffsetRange(
                message.offset, message.get_next_offset()
            )

    def close(self) -> None:
        self.__closed = True
        self.__step.close()

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

    def join(self, timeout: Optional[float] = None) -> None:
        if self.__batch is not None:
            self.__batch.join(timeout)
            logger.info("Completed processing %r.", self.__batch)
            self.__batch = None
