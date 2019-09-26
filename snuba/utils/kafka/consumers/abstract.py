from abc import ABC, abstractmethod
from typing import Any, Callable, Generic, Optional, Sequence, TypeVar

from confluent_kafka import TopicPartition


TMessage = TypeVar("TMessage")


class Consumer(Generic[TMessage], ABC):
    @abstractmethod
    def subscribe(
        self,
        topics: Sequence[str],
        on_assign: Optional[Callable[[Sequence[TopicPartition]], Any]] = None,
        on_revoke: Optional[Callable[[Sequence[TopicPartition]], Any]] = None,
    ) -> None:
        raise NotImplementedError

    @abstractmethod
    def poll(self, timeout: Optional[float] = None) -> Optional[TMessage]:
        raise NotImplementedError

    @abstractmethod
    def commit(self, asynchronous: bool = True) -> Optional[Sequence[TopicPartition]]:
        raise NotImplementedError

    @abstractmethod
    def close(self) -> None:
        raise NotImplementedError
