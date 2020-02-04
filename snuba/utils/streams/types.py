from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Generic, TypeVar


@dataclass(frozen=True, order=True)
class Topic:
    __slots__ = ["name"]

    name: str

    def __contains__(self, partition: Partition) -> bool:
        return partition.topic == self


@dataclass(frozen=True, order=True)
class Partition:
    __slots__ = ["topic", "index"]

    topic: Topic
    index: int


TPayload = TypeVar("TPayload")


@dataclass(frozen=True)
class Message(Generic[TPayload]):
    """
    Represents a single message within a partition.
    """

    __slots__ = ["partition", "offset", "payload", "timestamp"]

    partition: Partition
    offset: int
    payload: TPayload
    timestamp: datetime

    def get_next_offset(self) -> int:
        return self.offset + 1
