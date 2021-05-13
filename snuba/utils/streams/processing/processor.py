from __future__ import annotations

import logging
import time
from typing import Generic, Mapping, Optional, Sequence, Type

from snuba.utils.streams.backends.abstract import Consumer, ConsumerError
from snuba.utils.streams.metrics import DummyMetricsBackend, Metrics
from snuba.utils.streams.processing.strategies.abstract import (
    MessageRejected,
    ProcessingStrategy,
    ProcessingStrategyFactory,
)
from snuba.utils.streams.types import Message, Partition, Topic, TPayload

logger = logging.getLogger(__name__)

LOG_THRESHOLD_TIME = 10  # In seconds


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
        metrics: Metrics = DummyMetricsBackend,
        recoverable_errors: Optional[Sequence[Type[ConsumerError]]] = None,
    ) -> None:
        assert isinstance(metrics, Metrics)

        self.__consumer = consumer
        self.__processor_factory = processor_factory
        self.__metrics = metrics

        # The types passed to the `except` clause must be a tuple, not a Sequence.
        self.__recoverable_errors = tuple(recoverable_errors or [])

        self.__processing_strategy: Optional[ProcessingStrategy[TPayload]] = None

        self.__message: Optional[Message[TPayload]] = None

        # If the consumer is in the paused state, this is when the last call to
        # ``pause`` occurred.
        self.__paused_timestamp: Optional[float] = None
        self.__last_log_timestamp: Optional[float] = None

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
                        self.__paused_timestamp = time.time()
                    else:
                        # Log paused condition every 5 seconds at most
                        current_time = time.time()
                        if self.__last_log_timestamp:
                            paused_duration: Optional[
                                float
                            ] = current_time - self.__last_log_timestamp
                        elif self.__paused_timestamp:
                            paused_duration = current_time - self.__paused_timestamp
                        else:
                            paused_duration = None

                        if (
                            paused_duration is not None
                            and paused_duration > LOG_THRESHOLD_TIME
                        ):
                            self.__last_log_timestamp = current_time
                            logger.info(
                                "Paused for longer than %d seconds", LOG_THRESHOLD_TIME
                            )
                else:
                    # If we were trying to submit a message that failed to be
                    # submitted on a previous run, we can resume accepting new
                    # messages.
                    if message_carried_over:
                        assert self.__paused_timestamp is not None
                        paused_duration = time.time() - self.__paused_timestamp
                        self.__paused_timestamp = None
                        self.__last_log_timestamp = None
                        logger.debug(
                            "Successfully submitted %r, resuming consumer after %0.4f seconds...",
                            self.__message,
                            paused_duration,
                        )
                        self.__consumer.resume([*self.__consumer.tell().keys()])
                        self.__metrics.timing(
                            "pause_duration_ms", paused_duration * 1000
                        )
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
