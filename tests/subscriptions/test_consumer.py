import pytest

from snuba.subscriptions.consumer import Tick, TickConsumer
from snuba.utils.streams.consumer import Consumer, ConsumerError
from snuba.utils.streams.dummy import DummyConsumer, epoch
from snuba.utils.streams.types import Message, Partition, Topic
from snuba.utils.types import Interval


def test_tick_consumer() -> None:
    topic = Topic("messages")

    inner_consumer: Consumer[int] = DummyConsumer(
        {Partition(topic, 0): [0, 1, 2], Partition(topic, 1): [0]}
    )

    consumer = TickConsumer(inner_consumer)

    consumer.subscribe([topic])

    assert consumer.tell() == {
        Partition(topic, 0): 0,
        Partition(topic, 1): 0,
    }

    assert inner_consumer.tell() == {
        Partition(topic, 0): 0,
        Partition(topic, 1): 0,
    }

    # consume 0, 0
    assert consumer.poll() is None

    assert consumer.tell() == {
        Partition(topic, 0): 0,
        Partition(topic, 1): 0,
    }

    assert inner_consumer.tell() == {
        Partition(topic, 0): 1,
        Partition(topic, 1): 0,
    }

    # consume 0, 1
    assert consumer.poll() == Message(
        Partition(topic, 0),
        0,
        Tick(offsets=Interval(0, 1), timestamps=Interval(epoch, epoch)),
        epoch,
    )

    assert consumer.tell() == {
        Partition(topic, 0): 1,
        Partition(topic, 1): 0,
    }

    assert inner_consumer.tell() == {
        Partition(topic, 0): 2,
        Partition(topic, 1): 0,
    }

    # consume 0, 2
    assert consumer.poll() == Message(
        Partition(topic, 0),
        1,
        Tick(offsets=Interval(1, 2), timestamps=Interval(epoch, epoch)),
        epoch,
    )

    assert consumer.tell() == {
        Partition(topic, 0): 2,
        Partition(topic, 1): 0,
    }

    assert inner_consumer.tell() == {
        Partition(topic, 0): 3,
        Partition(topic, 1): 0,
    }

    # consume 1, 0
    assert consumer.poll() is None

    assert consumer.tell() == {
        Partition(topic, 0): 2,
        Partition(topic, 1): 0,
    }

    assert inner_consumer.tell() == {
        Partition(topic, 0): 3,
        Partition(topic, 1): 1,
    }

    # consume no message
    assert consumer.poll() is None

    assert consumer.tell() == {
        Partition(topic, 0): 2,
        Partition(topic, 1): 0,
    }

    assert inner_consumer.tell() == {
        Partition(topic, 0): 3,
        Partition(topic, 1): 1,
    }

    consumer.seek({Partition(topic, 0): 1})

    assert consumer.tell() == {
        Partition(topic, 0): 1,
        Partition(topic, 1): 0,
    }

    assert inner_consumer.tell() == {
        Partition(topic, 0): 1,
        Partition(topic, 1): 1,
    }

    # consume 0, 1
    assert consumer.poll() is None

    assert consumer.tell() == {
        Partition(topic, 0): 1,
        Partition(topic, 1): 0,
    }

    assert inner_consumer.tell() == {
        Partition(topic, 0): 2,
        Partition(topic, 1): 1,
    }

    # consume 0, 2
    assert consumer.poll() == Message(
        Partition(topic, 0),
        1,
        Tick(offsets=Interval(1, 2), timestamps=Interval(epoch, epoch)),
        epoch,
    )

    assert consumer.tell() == {
        Partition(topic, 0): 2,
        Partition(topic, 1): 0,
    }

    assert inner_consumer.tell() == {
        Partition(topic, 0): 3,
        Partition(topic, 1): 1,
    }

    with pytest.raises(ConsumerError):
        consumer.seek({Partition(topic, -1): 0})
