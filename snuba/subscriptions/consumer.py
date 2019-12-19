from datetime import datetime
from typing import (
    Any,
    Callable,
    Mapping,
    MutableMapping,
    NamedTuple,
    Optional,
    Sequence,
)

from snuba.utils.streams.consumer import Consumer
from snuba.utils.streams.types import Message, Partition, Topic
from snuba.utils.types import Interval


class MessageDetails(NamedTuple):
    offset: int
    timestamp: datetime


class Tick(NamedTuple):
    offsets: Interval[int]
    timestamps: Interval[datetime]


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
        self.__previous_messages: MutableMapping[
            Partition, Optional[MessageDetails]
        ] = {}

    def subscribe(
        self,
        topics: Sequence[Topic],
        on_assign: Optional[Callable[[Mapping[Partition, int]], None]] = None,
        on_revoke: Optional[Callable[[Sequence[Partition]], None]] = None,
    ) -> None:
        def assignment_callback(partitions: Mapping[Partition, int]) -> None:
            for partition in partitions:
                self.__previous_messages[partition] = None

            if on_assign is not None:
                on_assign(partitions)

        def revocation_callback(partitions: Sequence[Partition]) -> None:
            # TODO: This is probably not necessary -- this could be handled as
            # part of the assignment callback to avoid having to force reset
            # the partitions on rebalance (see the consumer implementation.)
            for partition in partitions:
                del self.__previous_messages[partition]

            if on_revoke is not None:
                on_revoke(partitions)

        self.__consumer.subscribe(
            topics, on_assign=assignment_callback, on_revoke=on_revoke
        )

    def poll(self, timeout: Optional[float] = None) -> Optional[Message[Tick]]:
        message = self.__consumer.poll(timeout)
        if message is None:
            return None

        previous_message = self.__previous_messages.get(message.partition)

        result: Optional[Message[Tick]]
        if previous_message is not None:
            result = Message(
                message.partition,
                previous_message.offset,
                Tick(
                    Interval(previous_message.offset, message.offset),
                    Interval(previous_message.timestamp, message.timestamp),
                ),
                previous_message.timestamp,
            )
        else:
            result = None

        self.__previous_messages[message.partition] = MessageDetails(
            message.offset, message.timestamp
        )

        return result

    def stage_offsets(self, offsets: Mapping[Partition, int]) -> None:
        return self.__consumer.stage_offsets(offsets)

    def commit_offsets(self) -> Mapping[Partition, int]:
        return self.__consumer.commit_offsets()

    def close(self, timeout: Optional[float] = None) -> None:
        return self.__consumer.close(timeout)


if __name__ == "__main__":
    from snuba.utils.streams.codecs import PassthroughCodec
    from snuba.utils.streams.consumer import KafkaConsumer

    c = KafkaConsumer(
        {
            "bootstrap.servers": "127.0.0.1:9092",
            "group.id": "asdf",
            "enable.auto.commit": "false",
            "enable.auto.offset.store": "false",
            "auto.offset.reset": "earliest",
        },
        codec=PassthroughCodec(),
    )
    t = TickConsumer(c)
    t.subscribe([Topic("test")])
