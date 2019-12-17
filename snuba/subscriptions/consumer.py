from datetime import datetime
from typing import Any, Callable, Mapping, MutableMapping, Optional, Sequence

from snuba.utils.streams.consumer import Consumer
from snuba.utils.streams.types import Message, Partition, Topic
from snuba.utils.types import Interval


class State:
    def __init__(self) -> None:
        self.__timestamp: Optional[datetime] = None

    def push(self, timestamp: datetime) -> Optional[Interval[datetime]]:
        if self.__timestamp is not None:
            interval: Interval[datetime] = Interval(self.__timestamp, timestamp)
            self.__timestamp = timestamp
            return interval
        else:
            self.__timestamp = timestamp
            return None


class TickConsumer(Consumer[Interval[datetime]]):
    """
    The ``TickConsumer`` is a ``Consumer`` implementation that differs from
    other ``Consumer`` implementations in that the messages returned returns
    contain a ``Tick`` that is derived from the timestamps of the previous
    two messages received within a partition.

    In other words, this consumer provides a measure of the progression of
    time, using the advancement of the broker timestamp within a Kafka
    partition as a "virtual clock" rather than depending on wall clock time.

    This consumer requires that the ``message.timestamp.type`` topic
    configuration is set to ``LogAppendTime`` for the topics that it is
    subscribed to, so that that the message time is set by the primary broker
    for the topic -- not the producer of the message -- ensuring that each
    partition timestamp moves monotonically.
    """

    # Since this consumer deals with the intervals *between* messages rather
    # the individual messages themselves, this introduces some additional
    # complexity into the way that offsets are managed. Take this example,
    # where a partition contains three messages:
    #
    #    Message:            A         B         C         D
    #    Offset:             0         1         2         3
    #    Timeline:   --------+---------+---------+---------+------>>>
    #
    # Consuming message "A" (via a call to ``poll``) does not cause a tick to
    # be returned, since an tick interval cannot be formed with the timestamp
    # from only one message. When message B is consumed, we can form a tick
    # interval using the timestamps from A and B.
    #
    # When storing (or committing) offsets, we need to be careful that we
    # correctly commit the offsets that represent the interval so that
    # intervals are not repeated or skipped when a consumer restarts (or more
    # likely rebalances, which can be generalized to a restart operation.)
    #
    # Take the previously described scenario where we had just returned a tick
    # interval that was represented by the mesasges A and B: without taking any
    # precautions, the Kafka consumer would use the next offset from message B
    # for commit, which would be 2 in this case (1 + 1). If the consumer were
    # to crash and restart, it would resume at offset 2, causing the next tick
    # interval returned to be for the messages C and D -- in this case, B and C
    # was never returned! To avoid skipping intervals when restarting, the
    # consumer would have had to commit the offset 1 (the offset of message B)
    # to ensure that upon restart, the next interval would be the interval
    # between B and C, since the message B was the first message received by
    # the consumer.

    def __init__(self, consumer: Consumer[Any]) -> None:
        self.__consumer = consumer

        self.__partitions: MutableMapping[Partition, State] = {}

    def subscribe(
        self,
        topics: Sequence[Topic],
        on_assign: Optional[Callable[[Mapping[Partition, int]], None]] = None,
        on_revoke: Optional[Callable[[Sequence[Partition]], None]] = None,
    ) -> None:
        def assignment_callback(partitions: Mapping[Partition, int]) -> None:
            for partition in partitions:
                self.__partitions[partition] = State()

            if on_assign is not None:
                on_assign(partitions)

        def revocation_callback(partitions: Sequence[Partition]) -> None:
            # TODO: This is probably not necessary.
            for partition in partitions:
                del self.__partitions[partition]

            if on_revoke is not None:
                on_revoke(partitions)

        self.__consumer.subscribe(
            topics, on_assign=assignment_callback, on_revoke=on_revoke
        )

    def poll(
        self, timeout: Optional[float] = None
    ) -> Optional[Message[Interval[datetime]]]:
        message = self.__consumer.poll(timeout)
        if message is None:
            return None

        interval = self.__partitions[message.partition].push(message.timestamp)
        if interval is None:
            return None

        # TODO: It might make sense to return the message offset from the
        # message on the lower side of the interval, since that message offset
        # most closely mirrors the behavior of Confluent Kafka driver's commit
        # method signature (message offset + 1) while retaining our desired
        # restart behavior.
        result: Message[Interval[datetime]] = Message(
            message.partition, message.offset, interval, message.timestamp,
        )

        # TODO: This needs to manually stage the offset of the message for
        # commit (if indicated by the consumer configuration.)

        return result

    def commit(self) -> Mapping[Partition, int]:
        return self.__consumer.commit()

    def close(self, timeout: Optional[float] = None) -> None:
        return self.__consumer.close(timeout)
