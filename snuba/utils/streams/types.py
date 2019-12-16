from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Generic, TypeVar


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
