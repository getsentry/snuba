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
        raise NotImplementedError

    @abstractmethod
    def poll(self, timeout: Optional[float] = None) -> Optional[TMessage]:
        raise NotImplementedError

    @abstractmethod
    def commit(
        self, asynchronous: bool = True
    ) -> Optional[Mapping[TopicPartitionKey, Offset]]:
        raise NotImplementedError

    @abstractmethod
    def close(self) -> None:
        raise NotImplementedError
