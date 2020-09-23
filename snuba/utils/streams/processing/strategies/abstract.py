from abc import ABC, abstractmethod
from typing import Callable, Generic, Mapping, Optional

from snuba.utils.streams.types import Message, Partition, TPayload


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
