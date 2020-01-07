from typing import cast

from snuba.subscriptions.consumer import Tick, TickConsumer
from snuba.utils.streams.consumer import Consumer
from snuba.utils.streams.dummy import DummyConsumer, epoch
from snuba.utils.streams.types import Message, Partition, Topic
from snuba.utils.types import Interval


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
