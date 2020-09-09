from __future__ import annotations

import logging
import time
from abc import ABC, abstractmethod
from typing import (
    Callable,
    Generic,
    Mapping,
    Optional,
    Sequence,
    Type,
)

from snuba.utils.streams.backends.abstract import Consumer, ConsumerError
from snuba.utils.streams.types import Message, Partition, Topic, TPayload


logger = logging.getLogger(__name__)


class MessageRejected(Exception):
    pass


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
        Poll the processor to check on the status of asynchronous tasks or
        perform other scheduled work.

        This method is called on each consumer loop iteration, so this method
        should not be used to perform work that may block for a significant
        amount of time and block the progress of the consumer or exceed the
        consumer poll interval timeout.

        This method may raise exceptions that were thrown by asynchronous
        tasks since the previous call to ``poll``.
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

        If the processing strategy is unable to accept a message (due to it
        being at or over capacity, for example), this method will raise a
        ``MessageRejected`` exception.
        """
        raise NotImplementedError

    @abstractmethod
    def close(self) -> None:
        """
        Close this instance. No more messages should be accepted by the
        instance after this method has been called.

        This method should not block. Once this strategy instance has
        finished processing (or discarded) all messages that were submitted
        prior to this method being called, the strategy should commit its
        partition offsets and release any resources that will no longer be
        used (threads, processes, sockets, files, etc.)
        """
        raise NotImplementedError

    @abstractmethod
    def terminate(self) -> None:
        """
        Close the processing strategy immediately, abandoning any work in
        progress. No more messages should be accepted by the instance after
        this method has been called.
        """
        raise NotImplementedError

    @abstractmethod
    def join(self, timeout: Optional[float] = None) -> None:
        """
        Block until the processing strategy has completed all previously
        submitted work, or the provided timeout has been reached. This method
        should be called after ``close`` to provide a graceful shutdown.

        This method is called synchronously by the stream processor during
        assignment revocation, and blocks the assignment from being released
        until this function exits, allowing any work in progress to be
        completed and committed before the continuing the rebalancing
        process.
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

        self.__message: Optional[Message[TPayload]] = None

        self.__shutdown_requested = False

        def on_partitions_assigned(partitions: Mapping[Partition, int]) -> None:
            if self.__processing_strategy is not None:
                raise InvalidStateError(
                    "received unexpected assignment with existing active processing strategy"
                )

            logger.info("New partitions assigned: %r", partitions)
            self.__processing_strategy = self.__processor_factory.create(self.__commit)
            logger.debug(
                "Initialized processing strategy: %r", self.__processing_strategy
            )

        def on_partitions_revoked(partitions: Sequence[Partition]) -> None:
            if self.__processing_strategy is None:
                raise InvalidStateError(
                    "received unexpected revocation without active processing strategy"
                )

            logger.info("Partitions revoked: %r", partitions)

            logger.debug("Closing %r...", self.__processing_strategy)
            self.__processing_strategy.close()

            logger.debug("Waiting for %r to exit...", self.__processing_strategy)
            self.__processing_strategy.join()

            logger.debug(
                "%r exited successfully, releasing assignment.",
                self.__processing_strategy,
            )
            self.__processing_strategy = None
            self.__message = None  # avoid leaking buffered messages across assignments

        self.__consumer.subscribe(
            [topic], on_assign=on_partitions_assigned, on_revoke=on_partitions_revoked
        )

    def __commit(self, offsets: Mapping[Partition, int]) -> None:
        self.__consumer.stage_offsets(offsets)
        start = time.time()
        self.__consumer.commit_offsets()
        logger.debug(
            "Waited %0.4f seconds for offsets to be committed to %r.",
            time.time() - start,
            self.__consumer,
        )

    def run(self) -> None:
        "The main run loop, see class docstring for more information."

        logger.debug("Starting")
        try:
            while not self.__shutdown_requested:
                self._run_once()

            self._shutdown()
        except Exception as error:
            logger.info("Caught %r, shutting down...", error)

            if self.__processing_strategy is not None:
                logger.debug("Terminating %r...", self.__processing_strategy)
                self.__processing_strategy.terminate()
                self.__processing_strategy = None

            logger.debug("Closing %r...", self.__consumer)
            self.__consumer.close()

            raise

    def _run_once(self) -> None:
        message_carried_over = self.__message is not None

        if message_carried_over:
            # If a message was carried over from the previous run, the consumer
            # should be paused and not returning any messages on ``poll``.
            if self.__consumer.poll(timeout=0) is not None:
                raise InvalidStateError(
                    "received message when consumer was expected to be paused"
                )
        else:
            # Otherwise, we need to try fetch a new message from the consumer,
            # even if there is no active assignment and/or processing strategy.
            try:
                self.__message = self.__consumer.poll(timeout=1.0)
            except self.__recoverable_errors:
                return

        if self.__processing_strategy is not None:
            self.__processing_strategy.poll()
            if self.__message is not None:
                try:
                    self.__processing_strategy.submit(self.__message)
                except MessageRejected as e:
                    # If the processing strategy rejected our message, we need
                    # to pause the consumer and hold the message until it is
                    # accepted, at which point we can resume consuming.
                    if not message_carried_over:
                        logger.debug(
                            "Caught %r while submitting %r, pausing consumer...",
                            e,
                            self.__message,
                        )
                        self.__consumer.pause([*self.__consumer.tell().keys()])
                else:
                    # If we were trying to submit a message that failed to be
                    # submitted on a previous run, we can resume accepting new
                    # messages.
                    if message_carried_over:
                        logger.debug(
                            "Successfully submitted %r, resuming consumer...",
                            self.__message,
                        )
                        self.__consumer.resume([*self.__consumer.tell().keys()])
                    self.__message = None
        else:
            if self.__message is not None:
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
