from __future__ import annotations

import logging
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import (
    Callable,
    Generic,
    Mapping,
    MutableMapping,
    MutableSequence,
    Optional,
    Sequence,
    TypeVar,
)

from snuba.utils.metrics.backends.abstract import MetricsBackend
from snuba.utils.streams.processing import ProcessingStrategy, ProcessingStrategyFactory
from snuba.utils.streams.types import Message, Partition, TPayload


logger = logging.getLogger(__name__)


TResult = TypeVar("TResult")


class AbstractBatchWorker(ABC, Generic[TPayload, TResult]):
    """
    The ``BatchProcessingStrategy`` requires an instance of this class to
    handle user provided work such as processing raw messages and flushing
    processed batches to a custom backend.
    """

    @abstractmethod
    def process_message(self, message: Message[TPayload]) -> Optional[TResult]:
        """
        Called with each raw message, allowing the worker to do incremental
        (preferably local!) work on events. The object returned is put into
        the batch maintained internally by the ``BatchProcessingStrategy``.

        If this method returns `None` it is not added to the batch.

        A simple example would be decoding the message payload value and
        extracting a few fields.
        """
        pass

    @abstractmethod
    def flush_batch(self, batch: Sequence[TResult]) -> None:
        """
        Called with a list of processed (by ``process_message``) objects.
        The worker should write the batch of processed messages into whatever
        store(s) it is maintaining. Afterwards the offsets are committed by
        the ``BatchProcessingStrategy``.

        A simple example would be writing the batch to another topic.
        """
        pass


@dataclass
class Offsets:
    __slots__ = ["lo", "hi"]

    lo: int  # inclusive
    hi: int  # exclusive


@dataclass
class Batch(Generic[TResult]):
    results: MutableSequence[TResult] = field(default_factory=list)
    offsets: MutableMapping[Partition, Offsets] = field(default_factory=dict)
    created: float = field(default_factory=lambda: time.time())
    messages_processed_count: int = 0
    # the total amount of time, in milliseconds, that it took to process
    # the messages in this batch (does not included time spent waiting for
    # new messages)
    processing_time_ms: float = 0.0


class BatchProcessingStrategy(ProcessingStrategy[TPayload]):
    """
    The ``BatchProcessingStrategy`` is a processing strategy that accumulates
    processed message values, periodically flushing them after a given
    duration of time has passed or number of output values have been
    accumulated. Users need only provide an implementation of what it means
    to process a raw message and flush a batch of events via an
    ``AbstractBatchWorker`` instance.

    Messages are processed as they are read from the consumer, then added to
    an in-memory batch. These batches are flushed based on the batch size or
    time sent since the first message in the batch was recieved (e.g. "500
    items or 1000ms"), whichever threshold is reached first. When a batch of
    items is flushed, the consumer offsets are synchronously committed.
    """

    def __init__(
        self,
        commit: Callable[[Mapping[Partition, int]], None],
        worker: AbstractBatchWorker[TPayload, TResult],
        max_batch_size: int,
        max_batch_time: int,
        metrics: MetricsBackend,
    ) -> None:
        self.__commit = commit
        self.__worker = worker
        self.__max_batch_size = max_batch_size
        self.__max_batch_time = max_batch_time
        self.__metrics = metrics

        self.__batch: Optional[Batch[TResult]] = None
        self.__closed = False

    def poll(self) -> None:
        """
        Check if the current in-flight batch should be flushed.
        """
        assert not self.__closed

        if self.__batch is not None and (
            len(self.__batch.results) >= self.__max_batch_size
            or time.time() > self.__batch.created + self.__max_batch_time / 1000.0
        ):
            self.__flush()

    def submit(self, message: Message[TPayload]) -> None:
        """
        Process a message.
        """
        assert not self.__closed

        start = time.time()

        self.__metrics.timing(
            "receive_latency",
            (start - message.timestamp.timestamp()) * 1000,
            tags={
                "topic": message.partition.topic.name,
                "partition": str(message.partition.index),
            },
        )

        # Create the batch only after the first message is seen.
        if self.__batch is None:
            self.__batch = Batch()

        result = self.__worker.process_message(message)

        # XXX: ``None`` is indistinguishable from a potentially valid return
        # value of ``TResult``!
        if result is not None:
            self.__batch.results.append(result)

        duration = (time.time() - start) * 1000
        self.__batch.messages_processed_count += 1
        self.__batch.processing_time_ms += duration
        self.__metrics.timing("process_message", duration)

        if message.partition in self.__batch.offsets:
            self.__batch.offsets[message.partition].hi = message.get_next_offset()
        else:
            self.__batch.offsets[message.partition] = Offsets(
                message.offset, message.get_next_offset()
            )

    def close(self) -> None:
        self.__closed = True

    def join(self, timeout: Optional[float] = None) -> None:
        # The active batch is discarded when exiting without attempting to
        # write or commit, so this method can exit immediately without
        # blocking.
        pass

    def __flush(self) -> None:
        """
        Flush the active batch and reset the batch state.
        """
        assert not self.__closed
        assert self.__batch is not None, "cannot flush without active batch"

        logger.info(
            "Flushing %s items (from %r)",
            len(self.__batch.results),
            self.__batch.offsets,
        )

        self.__metrics.timing(
            "process_message.normalized",
            self.__batch.processing_time_ms / self.__batch.messages_processed_count,
        )

        batch_results_length = len(self.__batch.results)
        if batch_results_length > 0:
            logger.debug("Flushing batch via worker")
            flush_start = time.time()
            self.__worker.flush_batch(self.__batch.results)
            flush_duration = (time.time() - flush_start) * 1000
            logger.info("Worker flush took %dms", flush_duration)
            self.__metrics.timing("batch.flush", flush_duration)
            self.__metrics.timing(
                "batch.flush.normalized", flush_duration / batch_results_length
            )

        logger.debug("Committing offsets for batch")
        commit_start = time.time()
        offsets = {
            partition: offsets.hi for partition, offsets in self.__batch.offsets.items()
        }
        self.__commit(offsets)
        logger.debug("Committed offsets: %s", offsets)
        commit_duration = (time.time() - commit_start) * 1000
        logger.debug("Offset commit took %dms", commit_duration)

        self.__batch = None


class BatchProcessingStrategyFactory(ProcessingStrategyFactory[TPayload]):
    def __init__(
        self,
        worker: AbstractBatchWorker[TPayload, TResult],
        max_batch_size: int,
        max_batch_time: int,
        metrics: MetricsBackend,
    ) -> None:
        self.__worker = worker
        self.__max_batch_size = max_batch_size
        self.__max_batch_time = max_batch_time
        self.__metrics = metrics

    def create(
        self, commit: Callable[[Mapping[Partition, int]], None]
    ) -> ProcessingStrategy[TPayload]:
        return BatchProcessingStrategy(
            commit,
            self.__worker,
            self.__max_batch_size,
            self.__max_batch_time,
            self.__metrics,
        )
