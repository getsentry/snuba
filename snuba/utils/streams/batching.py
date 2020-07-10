import logging
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from functools import partial
from typing import (
    Generic,
    Mapping,
    MutableMapping,
    MutableSequence,
    Optional,
    Sequence,
    Type,
    TypeVar,
)

from snuba.utils.metrics.backends.abstract import MetricsBackend
from snuba.utils.streams.consumer import Consumer, ConsumerError
from snuba.utils.streams.types import Message, Partition, Topic, TPayload

logger = logging.getLogger(__name__)


TResult = TypeVar("TResult")


class AbstractBatchWorker(ABC, Generic[TPayload, TResult]):
    """The `BatchingConsumer` requires an instance of this class to
    handle user provided work such as processing raw messages and flushing
    processed batches to a custom backend."""

    @abstractmethod
    def process_message(self, message: Message[TPayload]) -> Optional[TResult]:
        """Called with each raw message, allowing the worker to do
        incremental (preferably local!) work on events. The object returned
        is put into the batch maintained by the `BatchingConsumer`.

        If this method returns `None` it is not added to the batch.

        A simple example would be decoding the JSON value and extracting a few
        fields.
        """
        pass

    @abstractmethod
    def flush_batch(self, batch: Sequence[TResult]) -> None:
        """Called with a list of pre-processed (by `process_message`) objects.
        The worker should write the batch of processed messages into whatever
        store(s) it is maintaining. Afterwards the offsets are committed by
        the consumer.

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


class BatchProcessor:
    """
    The ``BatchProcessor`` is a message processor that accumulates processed
    message values, periodically flushing them after a given duration of time
    has passed or number of output values have been accumulated.
    """

    def __init__(
        self,
        consumer: Consumer[TPayload],
        worker: AbstractBatchWorker[TPayload, TResult],
        max_batch_size: int,
        max_batch_time: int,
        metrics: MetricsBackend,
    ) -> None:
        self.__consumer = consumer
        self.__worker = worker
        self.__max_batch_size = max_batch_size
        self.__max_batch_time = max_batch_time
        self.__metrics = metrics

        self.__batch: Optional[Batch[TResult]] = None
        self.__closed = False

    def process(self, message: Optional[Message[TPayload]]) -> None:
        """
        Process a message or heartbeat.

        Calling this method may trigger an in-flight batch to be flushed.
        """
        assert not self.__closed

        # If we have an active batch, check if it's ready to be flushed.
        if self.__batch is not None and (
            len(self.__batch.results) >= self.__max_batch_size
            or time.time() > self.__batch.created + self.__max_batch_time / 1000.0
        ):
            self.__flush()

        # If this was just a heartbeat, there is nothing to process and we can
        # skip the rest of the method.
        if message is None:
            return

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
        """
        Close the processor, discarding any messages (without committing
        offsets) that were previously consumed and processed since the last
        batch flush.
        """
        self.__closed = True

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
        self.__consumer.stage_offsets(
            {
                partition: offsets.hi
                for partition, offsets in self.__batch.offsets.items()
            }
        )
        offsets = self.__consumer.commit_offsets()
        logger.debug("Committed offsets: %s", offsets)
        commit_duration = (time.time() - commit_start) * 1000
        logger.debug("Offset commit took %dms", commit_duration)

        self.__batch = None


class BatchingConsumer(Generic[TPayload]):
    """The `BatchingConsumer` is an abstraction over the abstract Consumer's main event
    loop. For this reason it uses inversion of control: the user provides an implementation
    for the `AbstractBatchWorker` and then the `BatchingConsumer` handles the rest.

    * Messages are processed locally (e.g. not written to an external datastore!) as they are
      read from the consumer, then added to an in-memory batch
    * Batches are flushed based on the batch size or time sent since the first message
      in the batch was recieved (e.g. "500 items or 1000ms")
    * Consumer offsets are not automatically committed! If they were, offsets might be committed
      for messages that are still sitting in an in-memory batch, or they might *not* be committed
      when messages are sent to an external datastore right before the consumer process dies
    * Instead, when a batch of items is flushed they are written to the external datastore and
      then consumer offsets are immediately committed (in the same thread/loop)
    * Users need only provide an implementation of what it means to process a raw message
      and flush a batch of events

    NOTE: This does not eliminate the possibility of duplicates if the consumer process
    crashes between writing to its backend and commiting offsets. This should eliminate
    the possibility of *losing* data though. An "exactly once" consumer would need to store
    offsets in the external datastore and reconcile them on any partition rebalance.
    """

    def __init__(
        self,
        consumer: Consumer[TPayload],
        topic: Topic,
        worker: AbstractBatchWorker[TPayload, TResult],
        max_batch_size: int,
        max_batch_time: int,
        metrics: MetricsBackend,
        recoverable_errors: Optional[Sequence[Type[ConsumerError]]] = None,
    ) -> None:
        self.__consumer = consumer

        self.__processor_factory = partial(
            BatchProcessor, consumer, worker, max_batch_size, max_batch_time, metrics
        )

        self.__processor: Optional[BatchProcessor] = None

        self.__shutdown_requested = False

        # The types passed to the `except` clause must be a tuple, not a Sequence.
        self.__recoverable_errors = tuple(recoverable_errors or [])

        def on_partitions_assigned(partitions: Mapping[Partition, int]) -> None:
            assert (
                self.__processor is None
            ), "received unexpected assignment with existing active processor"

            logger.info("New partitions assigned: %r", partitions)
            self.__processor = self.__processor_factory()

        def on_partitions_revoked(partitions: Sequence[Partition]) -> None:
            "Reset the current in-memory batch, letting the next consumer take over where we left off."
            assert (
                self.__processor is not None
            ), "received unexpected revocation without active processor"

            logger.info("Partitions revoked: %r", partitions)
            self.__processor.close()
            self.__processor = None

        self.__consumer.subscribe(
            [topic], on_assign=on_partitions_assigned, on_revoke=on_partitions_revoked
        )

    def run(self) -> None:
        "The main run loop, see class docstring for more information."

        logger.debug("Starting")
        while not self.__shutdown_requested:
            self._run_once()

        self._shutdown()

    def _run_once(self) -> None:
        try:
            msg = self.__consumer.poll(timeout=1.0)
        except self.__recoverable_errors:
            return

        assert self.__processor is not None, "received message without active processor"
        self.__processor.process(msg)

    def signal_shutdown(self) -> None:
        """Tells the batching consumer to shutdown on the next run loop iteration.
        Typically called from a signal handler."""
        logger.debug("Shutdown signalled")

        self.__shutdown_requested = True

    def _shutdown(self) -> None:
        assert (
            self.__processor is not None
        ), "received shutdown request without active processor"

        logger.debug("Stopping processor")
        self.__processor.close()

        # close the consumer
        logger.debug("Stopping consumer")
        self.__consumer.close()
        logger.debug("Stopped")
