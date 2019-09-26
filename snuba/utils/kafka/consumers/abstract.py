from abc import ABC, abstractmethod
from typing import Callable, Generic, NewType, Optional, Sequence, Tuple, TypeVar


TMessage = TypeVar("TMessage")

Topic = NewType("Topic", str)
Partition = NewType("Partition", int)
Offset = NewType("Offset", int)


class Consumer(Generic[TMessage], ABC):
    @abstractmethod
    def subscribe(
        self,
        topics: Sequence[str],
        on_assign: Optional[Callable[[Sequence[Tuple[Topic, Partition]]], None]] = None,
        on_revoke: Optional[Callable[[Sequence[Tuple[Topic, Partition]]], None]] = None,
    ) -> None:
        raise NotImplementedError

    @abstractmethod
    def poll(self, timeout: Optional[float] = None) -> Optional[TMessage]:
        raise NotImplementedError

    @abstractmethod
    def commit(
        self, asynchronous: bool = True
    ) -> Optional[Sequence[Tuple[Topic, Partition, Offset]]]:
        raise NotImplementedError

    @abstractmethod
    def close(self) -> None:
        raise NotImplementedError
