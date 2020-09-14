from datetime import datetime, timedelta
from typing import Mapping, Optional

import pytest

from snuba.subscriptions.consumer import Tick, TickConsumer
from snuba.utils.clock import Clock
from snuba.utils.streams import ConsumerError, Message, Partition, Topic
from snuba.utils.streams.backends.dummy import DummyBroker
from snuba.utils.types import Interval
from tests.assertions import assert_changes, assert_does_not_change


def test_tick_time_shift() -> None:
    offsets = Interval(0, 1)
    tick = Tick(offsets, Interval(datetime(1970, 1, 1), datetime(1970, 1, 2)))
    assert tick.time_shift(timedelta(hours=24)) == Tick(
        offsets, Interval(datetime(1970, 1, 2), datetime(1970, 1, 3))
    )


@pytest.mark.parametrize(
    "time_shift",
    [
        pytest.param(None, id="without time shift"),
        pytest.param(timedelta(minutes=-5), id="with time shift"),
    ],
)
def test_tick_consumer(
    clock: Clock, broker: DummyBroker[int], time_shift: Optional[timedelta]
) -> None:
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
        Tick(offsets=Interval(0, 1), timestamps=Interval(epoch, epoch)).time_shift(
            time_shift
        ),
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
        Tick(offsets=Interval(1, 2), timestamps=Interval(epoch, epoch)).time_shift(
            time_shift
        ),
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
        Tick(offsets=Interval(1, 2), timestamps=Interval(epoch, epoch)).time_shift(
            time_shift
        ),
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


def test_tick_consumer_non_monotonic(clock: Clock, broker: DummyBroker[int]) -> None:
    epoch = datetime.fromtimestamp(clock.time())

    topic = Topic("messages")
    partition = Partition(topic, 0)

    broker.create_topic(topic, partitions=1)

    producer = broker.get_producer()

    inner_consumer = broker.get_consumer("group")

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
