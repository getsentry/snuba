from datetime import datetime
from typing import Callable, Mapping, MutableMapping, Optional, Sequence, cast

from snuba.subscriptions.consumer import Tick, TickConsumer
from snuba.utils.streams.consumer import Consumer, Message, Partition, Topic, TPayload
from snuba.utils.types import Interval

epoch = datetime(2019, 12, 19)


class DummyConsumer(Consumer[TPayload]):
    def __init__(self, messages: Mapping[Partition, Sequence[TPayload]]) -> None:
        # TODO: The message data needs to include the timestamp.
        self.__messages = messages

        self.__subscription: Sequence[Topic] = []
        self.__assignment: Optional[Sequence[Partition]] = None

        self.__offsets: MutableMapping[Partition, int] = {}
        self.__staged_offsets: MutableMapping[Partition, int] = {}
        self.__committed_offsets: MutableMapping[Partition, int] = {}

        self.__closed = False

    def subscribe(
        self,
        topics: Sequence[Topic],
        on_assign: Optional[Callable[[Mapping[Partition, int]], None]] = None,
        on_revoke: Optional[Callable[[Sequence[Partition]], None]] = None,
    ) -> None:
        assert not self.__closed

        self.__subscription = topics

        assignment: Sequence[Partition] = [
            partition
            for partition in self.__messages
            if any(partition in topic for topic in topics)
        ]

        if self.__assignment is not None and on_revoke is not None:
            on_revoke(self.__assignment)

        self.__assignment = assignment

        # TODO: Handle offset reset more realistically.
        self.__offsets = {
            partition: self.__committed_offsets.get(partition, 0)
            for partition in assignment
        }

        self.__staged_offsets.clear()

        if on_assign is not None:
            on_assign(self.__offsets)

    def poll(self, timeout: Optional[float] = None) -> Optional[Message[TPayload]]:
        assert not self.__closed

        # TODO: Throw ``EndOfPartition`` errors.
        for partition, offset in sorted(self.__offsets.items()):
            try:
                payload = self.__messages[partition][offset]
            except IndexError:
                pass
            else:
                message = Message(partition, offset, payload, epoch)
                self.__offsets[partition] = message.get_next_offset()
                return message

        return None

    def stage_offsets(self, offsets: Mapping[Partition, int]) -> None:
        assert not self.__closed

        self.__staged_offsets.update(offsets)

    def commit_offsets(self) -> Mapping[Partition, int]:
        assert not self.__closed

        offsets = self.__staged_offsets.copy()
        self.__committed_offsets.update(offsets)
        self.__staged_offsets.clear()
        return offsets

    def close(self, timeout: Optional[float] = None) -> None:
        self.__closed = True


def test_tick_consumer() -> None:
    topic = Topic("messages")

    consumer = TickConsumer(
        cast(
            Consumer[int],
            DummyConsumer({Partition(topic, 0): [0, 1, 2], Partition(topic, 1): [0]}),
        )
    )

    consumer.subscribe([topic])

    # consume 0, 0
    assert consumer.poll() is None

    # consume 0, 1
    assert consumer.poll() == Message(
        Partition(topic, 0),
        0,
        Tick(offsets=Interval(0, 1), timestamps=Interval(epoch, epoch)),
        epoch,
    )

    # consume 0, 2
    assert consumer.poll() == Message(
        Partition(topic, 0),
        1,
        Tick(offsets=Interval(1, 2), timestamps=Interval(epoch, epoch)),
        epoch,
    )

    # consume 1, 0
    assert consumer.poll() is None

    # consume no message
    assert consumer.poll() is None
