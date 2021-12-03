from datetime import datetime, timedelta
from typing import Mapping, Optional
from unittest import mock

import pytest
from arroyo import Message, Partition, Topic
from arroyo.backends.local.backend import LocalBroker as Broker
from arroyo.backends.local.storages.memory import MemoryMessageStorage
from arroyo.errors import ConsumerError
from arroyo.utils.clock import TestingClock

from snuba.subscriptions.consumer import TickConsumer
from snuba.subscriptions.utils import Tick
from snuba.utils.types import Interval
from tests.assertions import assert_changes, assert_does_not_change


@pytest.mark.parametrize(
    "time_shift",
    [
        pytest.param(None, id="without time shift"),
        pytest.param(timedelta(minutes=-5), id="with time shift"),
    ],
)
def test_tick_consumer(time_shift: Optional[timedelta]) -> None:
    clock = TestingClock()
    broker: Broker[int] = Broker(MemoryMessageStorage(), clock)

    epoch = datetime.fromtimestamp(clock.time())

    topic = Topic("messages")

    broker.create_topic(topic, partitions=2)

    producer = broker.get_producer()
    for partition, payloads in enumerate([[0, 1, 2], [0]]):
        for payload in payloads:
            producer.produce(Partition(topic, partition), payload).result()

    inner_consumer = broker.get_consumer("group")

    consumer = TickConsumer(inner_consumer, time_shift=time_shift)

    if time_shift is None:
        time_shift = timedelta()

    def _assignment_callback(offsets: Mapping[Partition, int]) -> None:
        assert consumer.tell() == {
            Partition(topic, 0): 0,
            Partition(topic, 1): 0,
        }

        assert inner_consumer.tell() == {
            Partition(topic, 0): 0,
            Partition(topic, 1): 0,
        }

    assignment_callback = mock.Mock(side_effect=_assignment_callback)

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
        Tick(
            None, offsets=Interval(0, 1), timestamps=Interval(epoch, epoch)
        ).time_shift(time_shift),
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
        Tick(
            None, offsets=Interval(1, 2), timestamps=Interval(epoch, epoch)
        ).time_shift(time_shift),
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
        Tick(
            None, offsets=Interval(1, 2), timestamps=Interval(epoch, epoch)
        ).time_shift(time_shift),
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
    clock = TestingClock()
    broker: Broker[int] = Broker(MemoryMessageStorage(), clock)

    epoch = datetime.fromtimestamp(clock.time())

    topic = Topic("messages")
    partition = Partition(topic, 0)

    broker.create_topic(topic, partitions=1)

    producer = broker.get_producer()

    inner_consumer = broker.get_consumer("group")

    consumer = TickConsumer(inner_consumer)

    def _assignment_callback(offsets: Mapping[Partition, int]) -> None:
        assert inner_consumer.tell() == {partition: 0}
        assert consumer.tell() == {partition: 0}

    assignment_callback = mock.Mock(side_effect=_assignment_callback)

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
                None,
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
                None,
                offsets=Interval(1, 3),
                timestamps=Interval(
                    epoch + timedelta(seconds=1), epoch + timedelta(seconds=2)
                ),
            ),
            epoch + timedelta(seconds=2),
        )


def test_tick_consumer_min_interval() -> None:
    clock = TestingClock()
    broker: Broker[int] = Broker(MemoryMessageStorage(), clock)

    topic = Topic("messages")

    broker.create_topic(topic, partitions=2)

    producer = broker.get_producer()
    for payload in range(3):
        producer.produce(Partition(topic, 0), payload).result()
        clock.sleep(1.0)

    inner_consumer = broker.get_consumer("group")

    consumer = TickConsumer(inner_consumer, min_interval=timedelta(seconds=2))

    consumer.subscribe([topic])

    assert consumer.poll() is None
    assert consumer.poll() is None
    message = consumer.poll()
    assert message is not None
    tick = message.payload
    assert tick.offsets.upper - tick.offsets.lower == 2
    assert tick.timestamps.upper - tick.timestamps.lower == timedelta(seconds=2)
