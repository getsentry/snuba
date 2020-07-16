from datetime import datetime, timedelta
from typing import Mapping

import pytest

from snuba.subscriptions.consumer import Tick, TickConsumer
from snuba.utils.clock import TestingClock
from snuba.utils.streams.consumer import Consumer, ConsumerError
from snuba.utils.streams.dummy import DummyBroker, DummyConsumer, DummyProducer, epoch
from snuba.utils.streams.types import Message, Partition, Topic
from snuba.utils.types import Interval
from tests.assertions import assert_changes, assert_does_not_change


def test_tick_time_shift() -> None:
    offsets = Interval(0, 1)
    tick = Tick(offsets, Interval(datetime(1970, 1, 1), datetime(1970, 1, 2)))
    assert tick.time_shift(timedelta(hours=24)) == Tick(
        offsets, Interval(datetime(1970, 1, 2), datetime(1970, 1, 3))
    )


def test_tick_consumer() -> None:
    topic = Topic("messages")

    broker: DummyBroker[int] = DummyBroker()
    broker.create_topic(topic, partitions=2)

    producer: DummyProducer[int] = DummyProducer(broker)
    for partition, payloads in enumerate([[0, 1, 2], [0]]):
        for payload in payloads:
            producer.produce(Partition(topic, partition), payload).result()

    inner_consumer: Consumer[int] = DummyConsumer(broker, "group")

    consumer = TickConsumer(inner_consumer)

    def assignment_callback(offsets: Mapping[Partition, int]) -> None:
        assignment_callback.called = True

        assert consumer.tell() == {
            Partition(topic, 0): 0,
            Partition(topic, 1): 0,
        }

        assert inner_consumer.tell() == {
            Partition(topic, 0): 0,
            Partition(topic, 1): 0,
        }

    assignment_callback.called = False

    consumer.subscribe([topic], on_assign=assignment_callback)

    with assert_changes(lambda: assignment_callback.called, False, True):
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


def test_tick_consumer_non_monotonic() -> None:
    topic = Topic("messages")
    partition = Partition(topic, 0)

    clock = TestingClock(epoch.timestamp())
    broker: DummyBroker[int] = DummyBroker(clock)
    broker.create_topic(topic, partitions=1)

    producer: DummyProducer[int] = DummyProducer(broker)

    inner_consumer: Consumer[int] = DummyConsumer(broker, "group")

    consumer = TickConsumer(inner_consumer)

    def assignment_callback(offsets: Mapping[Partition, int]) -> None:
        assignment_callback.called = True
        assert inner_consumer.tell() == {partition: 0}
        assert consumer.tell() == {partition: 0}

    assignment_callback.called = False

    consumer.subscribe([topic], on_assign=assignment_callback)

    producer.produce(partition, 0)

    clock.sleep(1)

    producer.produce(partition, 1)

    with assert_changes(lambda: assignment_callback.called, False, True):
        assert consumer.poll() is None

    assert inner_consumer.tell() == {partition: 1}
    assert consumer.tell() == {partition: 0}

    with assert_changes(
        inner_consumer.tell, {partition: 1}, {partition: 2}
    ), assert_changes(consumer.tell, {partition: 0}, {partition: 1}):
        assert consumer.poll() == Message(
            partition,
            0,
            Tick(
                offsets=Interval(0, 1),
                timestamps=Interval(epoch, epoch + timedelta(seconds=1)),
            ),
            epoch + timedelta(seconds=1),
        )

    clock.sleep(-1)

    producer.produce(partition, 2)

    with assert_changes(
        inner_consumer.tell, {partition: 2}, {partition: 3}
    ), assert_does_not_change(consumer.tell, {partition: 1}):
        assert consumer.poll() is None

    clock.sleep(2)

    producer.produce(partition, 3)

    with assert_changes(
        inner_consumer.tell, {partition: 3}, {partition: 4}
    ), assert_changes(consumer.tell, {partition: 1}, {partition: 3}):
        assert consumer.poll() == Message(
            partition,
            1,
            Tick(
                offsets=Interval(1, 3),
                timestamps=Interval(
                    epoch + timedelta(seconds=1), epoch + timedelta(seconds=2)
                ),
            ),
            epoch + timedelta(seconds=2),
        )
