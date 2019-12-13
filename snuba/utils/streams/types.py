from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class Topic:
    __slots__ = ["name"]

    name: str

    def __contains__(self, partition: Partition) -> bool:
        return partition.topic == self


@dataclass(frozen=True)
class Partition:
    __slots__ = ["topic", "index"]

    topic: Topic
    index: int


@dataclass(frozen=True)
class Payload:
    __slots__ = ["key", "value"]

    key: Optional[bytes]
    value: bytes


@dataclass(frozen=True)
class Message:
    """
    Represents a single message within a partition.
    """

    __slots__ = ["partition", "offset", "payload"]

    partition: Partition
    offset: int
    payload: Payload

    def get_next_offset(self) -> int:
        return self.offset + 1


class ConsumerError(Exception):
    """
    Base class for exceptions that are raised during consumption.

    Subclasses may extend this class to disambiguate errors that are specific
    to their implementation.
    """


class EndOfPartition(ConsumerError):
    """
    Raised when there are no more messages to consume from the partition.
    """

    def __init__(self, partition: Partition, offset: int):
        # The partition that the consumer has reached the end of.
        self.partition = partition

        # The next unconsumed offset in the partition (where there is currently
        # no message.)
        self.offset = offset
