from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Callable, Generic, Mapping, Optional, Sequence, TypeVar


# The ``TStream`` represents a stream of messages. This should not be a class
# used for interacting with the stream (that type of interaction should happen
# via the ``Consumer``), but this type should provide a means for the consumer
# to uniquely identify a stream. For example, if the stream backend is a file,
# this type should be the filename -- not the file object or file descriptor.
# If the stream is a Kafka partition, this should be the topic name and
# partition ID.
TStream = TypeVar("TStream")

# The ``TOffset`` represents a single message's position in the stream. For a
# file, this would be the seek position, or for a Kafka partition, this would
# be the partition offset. Offset types should have a total ordering.
TOffset = TypeVar("TOffset")

# The ``TValue`` is the type of data represented by the messages in this
# stream. This can be a bytes object or a string, but is generally more useful
# or this type to have more structure: a dataclass or some other object
# representation (after deserialization of the raw bytes) is generally more
# appropriate.
TValue = TypeVar("TValue")


@dataclass
class Message(Generic[TStream, TOffset, TValue]):
    """
    Represents a single message within a stream.
    """

    stream: TStream
    offset: TOffset
    value: TValue


class ConsumerError(Exception):
    """
    Base class for exceptions that are raised during consumption.

    Subclasses may extend this class to disambiguate errors that are specific
    to their implementation.
    """


class EndOfStream(ConsumerError, Generic[TStream]):
    """
    Raised when there are no more messages to consume from the stream.
    """

    def __init__(self, stream: TStream):
        self.stream = stream


class Consumer(ABC, Generic[TStream, TOffset, TValue]):
    """
    An abstract class that provides methods to consume messages from a
    multiplexed collection of streams.

    This interface is heavily "inspired" by the Confluent Kafka Consumer
    implementation, but only exposes a limited set of the available methods
    and several method signatures and/or return values differ -- these
    changes are intended to improve the ease of use of the consumer, as well
    as make development and testing more straightforward. Most code that uses
    the Confluent driver can be modified to use this interface with minimal
    changes, but it is not intended to be a drop-in replacement for the
    Confluent implementation.

    All methods are blocking unless otherise noted in the documentation for
    that method.
    """

    @abstractmethod
    def subscribe(
        self,
        topics: Sequence[str],
        on_assign: Optional[Callable[[Sequence[TStream]], None]] = None,
        on_revoke: Optional[Callable[[Sequence[TStream]], None]] = None,
    ) -> None:
        """
        Subscribe to topic streams. This replaces a previous subscription.
        This method does not block. The subscription may not be fulfilled
        immediately: instead, the ``on_assign`` and ``on_revoke`` callbacks
        are called when the subscription state changes with the updated
        assignment for this consumer.

        Raises a ``RuntimeError`` if called on a closed consumer.
        """
        raise NotImplementedError

    @abstractmethod
    def poll(
        self, timeout: Optional[float] = None
    ) -> Optional[Message[TStream, TOffset, TValue]]:
        """
        Return the next message available to be consumed, if one is
        available. If no message is available, this method will block up to
        the ``timeout`` value before returning ``None``. A timeout of
        ``0.0`` represents "do not block", while a timeout of ``None``
        represents "block until a message is available (or forever)".

        Calling this method may also invoke subscription state change
        callbacks.

        If the backend supports it, this method may raise a ``EndOfStream``
        error if there are no messages available to be consumed (and the
        backend is configured to do so.) This may also raise
        ``ConsumerError`` or one of it's subclasses, the meaning of which are
        implementation-dependent.

        Raises a ``RuntimeError`` if called on a closed consumer.
        """
        raise NotImplementedError

    @abstractmethod
    def commit(self) -> Mapping[TStream, TOffset]:
        """
        Commit staged offsets for all streams that this consumer is assigned
        to. The return value of this method is a mapping of streams with
        their committed offsets as values.

        Raises a ``RuntimeError`` if called on a closed consumer.
        """
        raise NotImplementedError

    @abstractmethod
    def close(self) -> None:
        """
        Close the consumer. This stops consuming messages, *may* commit
        staged offsets (depending on the implementation), and ends its
        subscription.

        Raises a ``RuntimeError`` if called on a closed consumer.
        """
        raise NotImplementedError
