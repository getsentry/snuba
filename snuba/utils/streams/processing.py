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


class ProcessingStrategy(ABC, Generic[TPayload]):
    """
    A processing strategy defines how a stream processor processes messages
    during the course of a single assignment. The processor is instantiated
    when the assignment is received, and closed when the assignment is
    revoked.

    This interface is intentionally not prescriptive, and affords a
    significant degree of flexibility for the various implementations.
    """

    @abstractmethod
    def poll(self) -> None:
        """
        Poll the processor.

        This method is called on each consumer loop iteration. It can be used
        to check on the status of asynchronous tasks or perform other
        scheduled tasks.
        """
        raise NotImplementedError

    @abstractmethod
    def submit(self, message: Message[TPayload]) -> None:
        """
        Submit a message for processing.

        Messages may be processed synchronously or asynchronously, depending
        on the implementation of the processing strategy. Callers of this
        method should not assume that this method returning successfully
        implies that the message was successfully processed.
        """
        raise NotImplementedError

    @abstractmethod
    def close(self) -> None:
        """
        Close this instance. No more messages will be received by the
        instance after this method has been called. This method is called
        synchronously by the stream processor during assignment revocation
        and blocks the assignment from being released until this function
        exits, making it a good place to wait for (or choose to abandon) any
        work in progress and commit partition offsets.

        This method also should be used to release any resources that will no
        longer be used (threads, processes, sockets, files, etc.) to avoid
        leaking resources, as well as performing any finalization tasks that
        may prevent this instance from being garbage collected.
        """
        raise NotImplementedError


class ProcessingStrategyFactory(ABC, Generic[TPayload]):
    @abstractmethod
    def create(
        self, commit: Callable[[Mapping[Partition, int]], None]
    ) -> ProcessingStrategy[TPayload]:
        """
        Instantiate and return a ``ProcessingStrategy`` instance.

        :param commit: A function that accepts a mapping of ``Partition``
        instances to offset values that should be committed.
        """
        raise NotImplementedError


class InvalidStateError(RuntimeError):
    pass


class StreamProcessor(Generic[TPayload]):
    """
    A stream processor manages the relationship between a ``Consumer``
    instance and a ``ProcessingStrategy``, ensuring that processing
    strategies are instantiated on partition assignment and closed on
    partition revocation.
    """

    def __init__(
        self,
        consumer: Consumer[TPayload],
        topic: Topic,
        processor_factory: ProcessingStrategyFactory[TPayload],
        recoverable_errors: Optional[Sequence[Type[ConsumerError]]] = None,
    ) -> None:
        self.__consumer = consumer
        self.__processor_factory = processor_factory

        # The types passed to the `except` clause must be a tuple, not a Sequence.
        self.__recoverable_errors = tuple(recoverable_errors or [])

        self.__processing_strategy: Optional[ProcessingStrategy[TPayload]] = None
        self.__shutdown_requested = False

        def on_partitions_assigned(partitions: Mapping[Partition, int]) -> None:
            if self.__processing_strategy is not None:
                raise InvalidStateError(
                    "received unexpected assignment with existing active processing strategy"
                )

            logger.info("New partitions assigned: %r", partitions)
            self.__processing_strategy = self.__processor_factory.create(self.__commit)

        def on_partitions_revoked(partitions: Sequence[Partition]) -> None:
            "Reset the current in-memory batch, letting the next consumer take over where we left off."
            if self.__processing_strategy is None:
                raise InvalidStateError(
                    "received unexpected revocation without active processing strategy"
                )

            logger.info("Partitions revoked: %r", partitions)
            self.__processing_strategy.close()
            self.__processing_strategy = None

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

        if self.__processing_strategy is not None:
            self.__processing_strategy.poll()
            if msg is not None:
                self.__processing_strategy.submit(msg)
        else:
            if msg is not None:
                raise InvalidStateError(
                    "received message without active processing strategy"
                )

    def signal_shutdown(self) -> None:
        """
        Tells the stream processor to shutdown on the next run loop
        iteration.

        Typically called from a signal handler.
        """
        logger.debug("Shutdown signalled")

        self.__shutdown_requested = True

    def _shutdown(self) -> None:
        # close the consumer
        logger.debug("Stopping consumer")
        self.__consumer.close()
        logger.debug("Stopped")

        # if there was an active processing strategy, it should be shut down
        # and unset when the partitions are revoked during consumer close
        assert (
            self.__processing_strategy is None
        ), "processing strategy was not closed on shutdown"
