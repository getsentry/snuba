from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import (
    Callable,
    Generic,
    Mapping,
    Optional,
    Sequence,
    Type,
)

from snuba.utils.streams.consumer import Consumer, ConsumerError
from snuba.utils.streams.types import Message, Partition, Topic, TPayload


logger = logging.getLogger(__name__)


class ProcessorFactory(ABC, Generic[TPayload]):
    @abstractmethod
    def create(
        self, commit: Callable[[Mapping[Partition, int]], None]
    ) -> Processor[TPayload]:
        raise NotImplementedError


class Processor(ABC, Generic[TPayload]):
    @abstractmethod
    def process(self, message: Optional[Message[TPayload]]) -> None:
        raise NotImplementedError

    @abstractmethod
    def close(self) -> None:
        raise NotImplementedError


class StreamProcessor(Generic[TPayload]):
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
        processor_factory: ProcessorFactory[TPayload],
        recoverable_errors: Optional[Sequence[Type[ConsumerError]]] = None,
    ) -> None:
        self.__consumer = consumer
        self.__processor_factory = processor_factory

        # The types passed to the `except` clause must be a tuple, not a Sequence.
        self.__recoverable_errors = tuple(recoverable_errors or [])

        self.__processor: Optional[Processor[TPayload]] = None
        self.__shutdown_requested = False

        def on_partitions_assigned(partitions: Mapping[Partition, int]) -> None:
            assert (
                self.__processor is None
            ), "received unexpected assignment with existing active processor"

            logger.info("New partitions assigned: %r", partitions)
            self.__processor = self.__processor_factory.create(self.__commit)

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

    def __commit(self, offsets: Mapping[Partition, int]) -> None:
        self.__consumer.stage_offsets(offsets)
        self.__consumer.commit_offsets()

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

        if self.__processor is not None:
            self.__processor.process(msg)
        else:
            assert msg is None, "received message without active processor"

    def signal_shutdown(self) -> None:
        """Tells the batching consumer to shutdown on the next run loop iteration.
        Typically called from a signal handler."""
        logger.debug("Shutdown signalled")

        self.__shutdown_requested = True

    def _shutdown(self) -> None:
        # close the consumer
        logger.debug("Stopping consumer")
        self.__consumer.close()
        logger.debug("Stopped")

        # if there was an active processor, it should be shut down and unset
        # when the partitions are revoked during consumer close
        assert self.__processor is None, "processor was not closed on shutdown"
