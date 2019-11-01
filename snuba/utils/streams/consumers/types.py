from abc import ABC, abstractmethod
from typing import Generic, TypeVar


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
