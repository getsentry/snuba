from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Callable, Generic, Mapping, Optional, Sequence, TypeVar


TStream = TypeVar("TStream")

TOffset = TypeVar("TOffset")

TValue = TypeVar("TValue")


@dataclass
class Message(Generic[TStream, TOffset, TValue]):
    stream: TStream
    offset: TOffset
    value: TValue


class ConsumerError(Exception):
    pass


class EndOfStream(ConsumerError, Generic[TStream]):
    def __init__(self, stream: TStream):
        self.stream = stream


class Consumer(ABC, Generic[TStream, TOffset, TValue]):
    @abstractmethod
    def subscribe(
        self,
        topics: Sequence[str],
        on_assign: Optional[Callable[[Sequence[TStream]], None]] = None,
        on_revoke: Optional[Callable[[Sequence[TStream]], None]] = None,
    ) -> None:
        raise NotImplementedError

    @abstractmethod
    def poll(
        self, timeout: Optional[float] = None
    ) -> Optional[Message[TStream, TOffset, TValue]]:
        raise NotImplementedError

    @abstractmethod
    def commit(self) -> Mapping[TStream, TOffset]:
        raise NotImplementedError

    @abstractmethod
    def close(self) -> None:
        raise NotImplementedError
