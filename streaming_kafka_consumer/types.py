from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Generic, Optional, TypeVar


@dataclass(order=True, unsafe_hash=True)
class Topic:
    __slots__ = ["name"]

    name: str

    def __contains__(self, partition: Partition) -> bool:
        return partition.topic == self


@dataclass(order=True, unsafe_hash=True)
class Partition:
    __slots__ = ["topic", "index"]

    topic: Topic
    index: int


TPayload = TypeVar("TPayload")


@dataclass(unsafe_hash=True)
class Message(Generic[TPayload]):
    """
    Represents a single message within a partition.
    """

    __slots__ = ["partition", "offset", "payload", "timestamp", "next_offset"]

    partition: Partition
    offset: int
    payload: TPayload
    timestamp: datetime
    next_offset: int

    def __init__(
        self,
        partition: Partition,
        offset: int,
        payload: TPayload,
        timestamp: datetime,
        next_offset: Optional[int] = None,
    ) -> None:
        if next_offset is None:
            next_offset = offset + 1

        self.partition = partition
        self.offset = offset
        self.payload = payload
        self.timestamp = timestamp
        self.next_offset = next_offset

    def __repr__(self) -> str:
        # XXX: Field values can't be excluded from ``__repr__`` with
        # ``dataclasses.field(repr=False)`` as this class is defined with
        # ``__slots__`` for performance reasons. The class variable names
        # would conflict with the instance slot names, causing an error.
        return f"{type(self).__name__}(partition={self.partition!r}, offset={self.offset!r})"
