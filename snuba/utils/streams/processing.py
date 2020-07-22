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

        # if there was an active processor, it should be shut down and unset
        # when the partitions are revoked during consumer close
        assert self.__processor is None, "processor was not closed on shutdown"
