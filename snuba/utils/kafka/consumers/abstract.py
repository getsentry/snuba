from abc import ABC, abstractmethod
from typing import (
    Callable,
    Generic,
    Mapping,
    NamedTuple,
    NewType,
    Optional,
    Sequence,
    TypeVar,
)


TMessage = TypeVar("TMessage")


class TopicPartitionKey(NamedTuple):
    topic: str
    partition: int


Offset = NewType("Offset", int)


class Consumer(Generic[TMessage], ABC):
    """
    An abstract class that provides methods to consume messages (or data
    derived from messages) from a Kafka cluster.

    This interface is heavily "inspired" by the Confluent Kafka Consumer
    implementation, but only exposes a limited set of the available methods.
    All methods are blocking unless noted in the method documentation itself.

    This interface also includes several minor changes that are intended to
    improve ease of use and improve the quality of the development and
    testing experience through type annotations. Most code that uses the
    Confluent driver can be modified to use this interface with minimal
    changes, but it is not intended to be a drop-in replacement.
    """

    @abstractmethod
    def subscribe(
        self,
        topics: Sequence[str],
        on_assign: Optional[Callable[[Sequence[TopicPartitionKey]], None]] = None,
        on_revoke: Optional[Callable[[Sequence[TopicPartitionKey]], None]] = None,
    ) -> None:
        """
        Subscribe to topics. This replaces a previous subscription. This
        method does not block.

        The ``on_assign`` and ``on_revoke`` callbacks are called when the
        subscription state changes with the updated assignment for this
        consumer.

        Raises a ``RuntimeError`` if called on a closed consumer.
        """
        raise NotImplementedError

    @abstractmethod
    def poll(self, timeout: Optional[float] = None) -> Optional[TMessage]:
        """
        Return the next message available to be consumed, if available. If no
        message is available, this method will block up to the ``timeout``
        value: ``0.0`` represents "do not block", while ``None`` represents
        "block until a message is available (or forever)".

        Calling this method may also invoke subscription state change
        callbacks.

        Raises a ``RuntimeError`` if called on a closed consumer.
        """
        raise NotImplementedError

    @abstractmethod
    def commit(self) -> Mapping[TopicPartitionKey, Offset]:
        """
        Synchronously commit staged offsets for all partitions in this
        consumer's assignment set. Returns a mapping of TopicPartitionKey
        with the committed offsets (the next offset that would be yielded
        after restart for a partition) as values.

        Raises a ``RuntimeError`` if called on a closed consumer.
        """
        raise NotImplementedError

    @abstractmethod
    def close(self) -> None:
        """
        Close the consumer. This stops consuming messages, *may* commit
        staged offsets (depending on the implementation), and leaves the
        consumer group.

        Raises a ``RuntimeError`` if called on a closed consumer.
        """
        raise NotImplementedError
