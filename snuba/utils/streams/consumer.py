import logging
from abc import ABC, abstractmethod
from typing import (
    Callable,
    Generic,
    Mapping,
    Optional,
    Sequence,
)

from snuba.utils.streams.types import (
    Message,
    Partition,
    Topic,
    TPayload,
)


logger = logging.getLogger(__name__)


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


class Consumer(Generic[TPayload], ABC):
    """
    This abstract class provides an interface for consuming messages from a
    multiplexed collection of partitioned topic streams.

    Partitions support sequential access, as well as random access by
    offsets. There are three types of offsets that a consumer interacts with:
    working offsets, staged offsets, and committed offsets. Offsets always
    represent the starting offset of the *next* message to be read. (For
    example, committing an offset of X means the next message fetched via
    poll will have a least an offset of X, and the last message read had an
    offset less than X.)

    The working offsets are used track the current read position within a
    partition. This can be also be considered as a cursor, or as high
    watermark. Working offsets are local to the consumer process. They are
    not shared with other consumer instances in the same consumer group and
    do not persist beyond the lifecycle of the consumer instance, unless they
    are committed.

    Committed offsets are managed by an external arbiter/service, and are
    used as the starting point for a consumer when it is assigned a partition
    during the subscription process. To ensure that a consumer roughly "picks
    up where it left off" after restarting, or that another consumer in the
    same group doesn't read messages that have been processed by another
    consumer within the same group during a rebalance operation, offsets must
    be regularly committed by calling ``commit_offsets`` after they have been
    staged with ``stage_offsets``. Offsets are not staged or committed
    automatically!

    During rebalance operations, working offsets are rolled back to the
    latest committed offset for a partition, and staged offsets are cleared
    after the revocation callback provided to ``subscribe`` is called. (This
    occurs even if the consumer retains ownership of the partition across
    assignments.) For this reason, it is generally good practice to ensure
    offsets are committed as part of the revocation callback.
    """

    @abstractmethod
    def subscribe(
        self,
        topics: Sequence[Topic],
        on_assign: Optional[Callable[[Mapping[Partition, int]], None]] = None,
        on_revoke: Optional[Callable[[Sequence[Partition]], None]] = None,
    ) -> None:
        raise NotImplementedError

    @abstractmethod
    def poll(self, timeout: Optional[float] = None) -> Optional[Message[TPayload]]:
        raise NotImplementedError

    @abstractmethod
    def tell(self) -> Mapping[Partition, int]:
        """
        Return the working offsets for all currently assigned positions.
        """
        raise NotImplementedError

    @abstractmethod
    def seek(self, offsets: Mapping[Partition, int]) -> None:
        """
        Update the working offsets for the provided partitions.

        When using this method, it is possible to set a partition to an
        invalid offset without an immediate error. (Examples of invalid
        offsets include an offset that is too low and has already been
        dropped by the broker due to data retention policies, or an offset
        that is too high which is not yet associated with a message.) Since
        this method only updates the local working offset (and does not
        communicate with the broker), setting an invalid offset will cause a
        subsequent ``poll`` call to raise an exception, even though the call
        to ``seek`` succeeded.

        If any provided partitions are not in the assignment set, an
        exception will be raised and no offsets will be modified.
        """
        raise NotImplementedError

    @abstractmethod
    def stage_offsets(self, offsets: Mapping[Partition, int]) -> None:
        """
        Stage offsets to be committed. If an offset has already been staged
        for a given partition, that offset is overwritten (even if the offset
        moves in reverse.)
        """
        raise NotImplementedError

    @abstractmethod
    def commit_offsets(self) -> Mapping[Partition, int]:
        """
        Commit staged offsets. The return value of this method is a mapping
        of streams with their committed offsets as values.
        """
        raise NotImplementedError

    @abstractmethod
    def close(self, timeout: Optional[float] = None) -> None:
        raise NotImplementedError
