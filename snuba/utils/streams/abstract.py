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
# for this type to have more structure: a dataclass or some other object
# representation (after deserialization of the raw bytes) is generally more
# appropriate.
TValue = TypeVar("TValue")


class Message(ABC, Generic[TStream, TOffset, TValue]):
    """
    Represents a single message within a stream.
    """

    def __init__(self, stream: TStream, offset: TOffset, value: TValue) -> None:
        self.stream = stream
        self.offset = offset
        self.value = value

    def __repr__(self) -> str:
        return f"{type(self).__name__}(stream={self.stream!r}, offset={self.offset!r})"

    @abstractmethod
    def get_next_offset(self) -> TOffset:
        """
        Return the offset of the next message in this stream.
        """
        raise NotImplementedError


class ConsumerError(Exception):
    """
    Base class for exceptions that are raised during consumption.

    Subclasses may extend this class to disambiguate errors that are specific
    to their implementation.
    """


class EndOfStream(ConsumerError, Generic[TStream, TOffset]):
    """
    Raised when there are no more messages to consume from the stream.
    """

    def __init__(self, stream: TStream, offset: TOffset):
        # The stream that the consumer has reached the end of.
        self.stream = stream
        # The next unconsumed offset (where there is currently no message.)
        self.offset = offset


class Consumer(ABC, Generic[TStream, TOffset, TValue]):
    """
    An abstract class that provides methods to consume messages from a
    multiplexed collection of streams.

    Streams support sequential access, as well as random access by offsets.
    The specific data types of offsets are implementation dependent, but
    offset values should represent a totally-ordered, monotonic sequence.
    Offsets are often numeric, such as indexes into sequences or byte offsets
    into a file.

    There are three types of offsets: working offsets, staged offsets, and
    committed offsets. Working offsets are used to track a high watermark of
    the messages that this consumer has read (but not necessarily taken
    action for, such as writing to a database) within a specific stream.
    Working offsets are local to the consumer process. Staged offsets are
    also local to the consumer process, and are used to track a high
    watermark of the messages that this consumer *has* taken action for
    (again, such as writing to a database.) Committed offsets are managed by
    an external (implementation dependent) arbiter, and are used as the
    starting point for a consumer when it is assigned a stream during the
    subscription process.

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
    def unsubscribe(self) -> None:
        """
        Unsubscribe from streams.

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

        This method may raise ``ConsumerError`` or one of it's
        subclasses, the specific meaning of which are backend implementation
        specific.

        This method may also raise an ``EndOfStream`` error (a subtype of
        ``ConsumerError``) when the consumer has reached the end of a stream
        that it is subscribed to and no additional messages are available.
        The ``stream`` attribute of the raised exception specifies the end
        which stream has been reached. (Since this consumer is multiplexing a
        set of streams, this exception does not mean that *all* of the
        streams that the consumer is subscribed to do not have any messages,
        just that it has reached the end of one of them. This also does not
        mean that additional messages won't be available in future poll
        calls.) Not every backend implementation supports this feature or is
        configured to raise in this scenario.

        Raises a ``RuntimeError`` if called on a closed consumer.
        """
        raise NotImplementedError

    @abstractmethod
    def tell(self) -> Mapping[TStream, TOffset]:
        """
        Return the working offsets for all assigned streams.

        Raises a ``RuntimeError`` if called on a closed consumer.
        """
        raise NotImplementedError

    @abstractmethod
    def seek(self, offsets: Mapping[TStream, TOffset]) -> None:
        """
        Change the working offsets for the provided streams.

        Raises a ``RuntimeError`` if called on a closed consumer.
        """
        raise NotImplementedError

    @abstractmethod
    def stage_offsets(self, offsets: Mapping[TStream, TOffset]) -> None:
        """
        Stage offsets for the next commit.

        Raises a ``RuntimeError`` if called on a closed consumer.
        """
        raise NotImplementedError

    @abstractmethod
    def commit_offsets(self) -> Mapping[TStream, TOffset]:
        """
        Commit staged offsets. The return value of this method is a mapping
        of streams with their committed offsets as values.

        Raises a ``RuntimeError`` if called on a closed consumer.
        """
        raise NotImplementedError

    @abstractmethod
    def close(self, timeout: Optional[float] = None) -> None:
        """
        Close the consumer. This stops consuming messages, *may* commit
        staged offsets (depending on the implementation), and ends its
        subscription.

        Raises a ``TimeoutError`` if the consumer is unable to be closed
        before the timeout is reached.
        """
        raise NotImplementedError
