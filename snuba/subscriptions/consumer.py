from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import (
    Any,
    Callable,
    Mapping,
    MutableMapping,
    NamedTuple,
    Optional,
    Sequence,
)

from snuba.utils.streams.abstract import Consumer, ConsumerError
from snuba.utils.streams.types import Message, Partition, Topic
from snuba.utils.types import Interval, InvalidRangeError


logger = logging.getLogger(__name__)


class MessageDetails(NamedTuple):
    offset: int
    timestamp: datetime


class Tick(NamedTuple):
    offsets: Interval[int]
    timestamps: Interval[datetime]

    def time_shift(self, delta: timedelta) -> Tick:
        """
        Returns a new ``Tick`` instance that has had the bounds of its time
        interval shifted by the provided delta.
        """
        return Tick(
            self.offsets,
            Interval(self.timestamps.lower + delta, self.timestamps.upper + delta),
        )


class TickConsumer(Consumer[Tick]):
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
        self.__previous_messages: MutableMapping[Partition, MessageDetails] = {}

    def subscribe(
        self,
        topics: Sequence[Topic],
        on_assign: Optional[Callable[[Mapping[Partition, int]], None]] = None,
        on_revoke: Optional[Callable[[Sequence[Partition]], None]] = None,
    ) -> None:
        def revocation_callback(partitions: Sequence[Partition]) -> None:
            for partition in partitions:
                if partition in self.__previous_messages:
                    del self.__previous_messages[partition]

            if on_revoke is not None:
                on_revoke(partitions)

        self.__consumer.subscribe(
            topics, on_assign=on_assign, on_revoke=revocation_callback
        )

    def unsubscribe(self) -> None:
        self.__consumer.unsubscribe()

    def poll(self, timeout: Optional[float] = None) -> Optional[Message[Tick]]:
        message = self.__consumer.poll(timeout)
        if message is None:
            return None

        previous_message = self.__previous_messages.get(message.partition)

        result: Optional[Message[Tick]]
        if previous_message is not None:
            try:
                time_interval = Interval(previous_message.timestamp, message.timestamp)
            except InvalidRangeError:
                logger.warning(
                    "Could not construct valid time interval between %r and %r!",
                    previous_message,
                    message,
                    exc_info=True,
                )
                return None
            else:
                result = Message(
                    message.partition,
                    previous_message.offset,
                    Tick(
                        Interval(previous_message.offset, message.offset),
                        time_interval,
                    ),
                    message.timestamp,
                )
        else:
            result = None

        self.__previous_messages[message.partition] = MessageDetails(
            message.offset, message.timestamp
        )

        return result

    def pause(self, partitions: Sequence[Partition]) -> None:
        self.__consumer.pause(partitions)

    def resume(self, partitions: Sequence[Partition]) -> None:
        self.__consumer.resume(partitions)

    def paused(self) -> Sequence[Partition]:
        return self.__consumer.paused()

    def tell(self) -> Mapping[Partition, int]:
        # If there is no previous message for a partition, return the current
        # consumer offset, otherwise return the previous message offset (which
        # will be the next offset returned for that partition) to make the
        # behavior of the consumer consistent with what would typically be
        # expected by the caller.
        return {
            partition: (
                self.__previous_messages[partition].offset
                if partition in self.__previous_messages
                else offset
            )
            for partition, offset in self.__consumer.tell().items()
        }

    def seek(self, offsets: Mapping[Partition, int]) -> None:
        if offsets.keys() - self.__consumer.tell().keys():
            raise ConsumerError("cannot seek on unassigned partitions")

        for partition in offsets:
            if partition in self.__previous_messages:
                del self.__previous_messages[partition]

        return self.__consumer.seek(offsets)

    def stage_offsets(self, offsets: Mapping[Partition, int]) -> None:
        return self.__consumer.stage_offsets(offsets)

    def commit_offsets(self) -> Mapping[Partition, int]:
        return self.__consumer.commit_offsets()

    def close(self, timeout: Optional[float] = None) -> None:
        return self.__consumer.close(timeout)

    @property
    def closed(self) -> bool:
        return self.__consumer.closed
