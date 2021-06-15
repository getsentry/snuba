from arroyo.types import Partition


class ConsumerError(Exception):
    """
    Base class for exceptions that are raised during consumption.

    Subclasses may extend this class to disambiguate errors that are specific
    to their implementation.
    """


class RecoverableError(ConsumerError):
    """
    Recoverable consumer error
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


class OffsetOutOfRange(ConsumerError):
    """
    Raised when trying to read from an invalid position in the partition.
    """


class TransportError(RecoverableError):
    """
    Transport/connection failure. May be intermittent.
    """
