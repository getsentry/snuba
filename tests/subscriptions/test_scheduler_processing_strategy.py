from datetime import datetime, timedelta
from unittest import mock

from arroyo import Message, Partition, Topic

from snuba.subscriptions.scheduler_processing_strategy import (
    CommittableTick,
    ProvideCommitStrategy,
    TickBuffer,
)
from snuba.subscriptions.utils import SchedulingWatermarkMode, Tick
from snuba.utils.types import Interval
from tests.backends.metrics import TestingMetricsBackend, Timing


def test_tick_buffer_immediate() -> None:
    epoch = datetime(1970, 1, 1)

    metrics_backend = TestingMetricsBackend()

    next_step = mock.Mock()

    strategy = TickBuffer(
        SchedulingWatermarkMode.PARTITION, 2, None, next_step, metrics_backend
    )

    topic = Topic("messages")
    partition = Partition(topic, 0)

    message = Message(
        partition,
        4,
        Tick(
            0,
            offsets=Interval(1, 3),
            timestamps=Interval(epoch, epoch + timedelta(seconds=5)),
        ),
        epoch,
        5,
    )

    strategy.submit(message)

    assert next_step.submit.call_count == 1
    assert next_step.submit.call_args == mock.call(message)
    assert metrics_backend.calls == []


def test_tick_buffer_wait_slowest() -> None:
    epoch = datetime(1970, 1, 1)
    now = datetime.now()

    metrics_backend = TestingMetricsBackend()

    next_step = mock.Mock()

    # Create strategy with 2 partitions
    strategy = TickBuffer(
        SchedulingWatermarkMode.GLOBAL, 2, 10, next_step, metrics_backend,
    )

    topic = Topic("messages")
    commit_log_partition = Partition(topic, 0)

    # First message in partition 0, do not submit to next step
    message_0_0 = Message(
        commit_log_partition,
        4,
        Tick(
            0,
            offsets=Interval(1, 3),
            timestamps=Interval(epoch, epoch + timedelta(seconds=5)),
        ),
        now,
        5,
    )
    strategy.submit(message_0_0)

    assert next_step.submit.call_count == 0
    assert metrics_backend.calls == []

    # Another message in partition 0, do not submit to next step
    message_0_1 = Message(
        commit_log_partition,
        5,
        Tick(
            0,
            offsets=Interval(3, 4),
            timestamps=Interval(
                epoch + timedelta(seconds=5), epoch + timedelta(seconds=10)
            ),
        ),
        now,
        6,
    )
    strategy.submit(message_0_1)

    assert next_step.submit.call_count == 0
    assert metrics_backend.calls == []

    # Message in partition 1 has the lowest timestamp so we submit to the next step
    message_1_0 = Message(
        commit_log_partition,
        6,
        Tick(
            1,
            offsets=Interval(100, 120),
            timestamps=Interval(epoch, epoch + timedelta(seconds=4)),
        ),
        now,
        7,
    )
    strategy.submit(message_1_0)

    assert next_step.submit.call_count == 1
    assert next_step.submit.call_args_list == [mock.call(message_1_0)]
    assert metrics_backend.calls == [
        Timing("partition_lag_ms", 6000.0, None),
    ]

    next_step.reset_mock()
    metrics_backend.calls = []
    # Message in partition 1 has the same timestamp as the earliest message
    # in partition 0. Both should be submitted to the next step.
    message_1_1 = Message(
        commit_log_partition,
        7,
        Tick(
            1,
            offsets=Interval(120, 130),
            timestamps=Interval(
                epoch + timedelta(seconds=4), epoch + timedelta(seconds=5)
            ),
        ),
        now,
        8,
    )
    strategy.submit(message_1_1)

    assert next_step.submit.call_count == 2
    assert next_step.submit.call_args_list == [
        mock.call(message_0_0),
        mock.call(message_1_1),
    ]
    assert metrics_backend.calls == [
        Timing("partition_lag_ms", 5000.0, None),
    ]

    next_step.reset_mock()
    metrics_backend.calls = []

    # Submit another message to partition 1 with the same timestamp as
    # in partition 0. Two more messages should be submitted and the
    # the partition lag should be 0 now.
    message_1_2 = Message(
        commit_log_partition,
        7,
        Tick(
            1,
            offsets=Interval(130, 140),
            timestamps=Interval(
                epoch + timedelta(seconds=5), epoch + timedelta(seconds=10)
            ),
        ),
        now,
        8,
    )
    strategy.submit(message_1_2)

    assert next_step.submit.call_count == 2
    assert next_step.submit.call_args_list == [
        mock.call(message_0_1),
        mock.call(message_1_2),
    ]
    assert metrics_backend.calls == [
        Timing("partition_lag_ms", 0.0, None),
    ]

    next_step.reset_mock()
    metrics_backend.calls = []

    # Submit 11 more messages to partition 0. Since we hit
    # `max_ticks_buffered_per_partition`, the first message (but
    # none of the others) should be submitted to the next step.
    messages = []
    for i in range(11):
        message = Message(
            commit_log_partition,
            8 + i,
            Tick(
                1,
                offsets=Interval(4 + i, 5 + i),
                timestamps=Interval(
                    epoch + timedelta(seconds=10 + i),
                    epoch + timedelta(seconds=11 + i),
                ),
            ),
            now + timedelta(seconds=i),
            9 + i,
        )
        messages.append(message)
        strategy.submit(message)

    assert next_step.submit.call_count == 1
    assert next_step.submit.call_args_list == [mock.call(messages[0])]
    assert metrics_backend.calls == []


def make_message_for_next_step(
    message: Message[Tick], should_commit: bool
) -> Message[CommittableTick]:
    return Message(
        message.partition,
        message.offset,
        CommittableTick(message.payload, should_commit),
        message.timestamp,
        message.next_offset,
    )


def test_provide_commit_strategy() -> None:
    epoch = datetime(1970, 1, 1)
    next_step = mock.Mock()
    strategy = ProvideCommitStrategy(2, next_step)

    topic = Topic("messages")
    partition = Partition(topic, 0)

    # First message for partition 0 -> do not commit offset
    message_0_0 = Message(
        partition,
        1,
        Tick(
            0,
            offsets=Interval(1, 2),
            timestamps=Interval(
                epoch + timedelta(seconds=1), epoch + timedelta(seconds=2)
            ),
        ),
        epoch,
        2,
    )

    strategy.submit(message_0_0)
    assert next_step.submit.call_args_list == [
        mock.call(make_message_for_next_step(message_0_0, False))
    ]

    next_step.reset_mock()

    # Partition 1, don't commit since timestamp is higher than partition 0
    message_1_0 = Message(
        partition,
        2,
        Tick(
            1,
            offsets=Interval(11, 12),
            timestamps=Interval(
                epoch + timedelta(seconds=2), epoch + timedelta(seconds=3)
            ),
        ),
        epoch,
        3,
    )

    strategy.submit(message_1_0)

    assert next_step.submit.call_args_list == [
        mock.call(make_message_for_next_step(message_1_0, False))
    ]

    next_step.reset_mock()

    # Partition 1, another higher timestamp
    message_1_1 = Message(
        partition,
        3,
        Tick(
            1,
            offsets=Interval(12, 13),
            timestamps=Interval(
                epoch + timedelta(seconds=3), epoch + timedelta(seconds=6)
            ),
        ),
        epoch,
        4,
    )

    strategy.submit(message_1_1)

    assert next_step.submit.call_args_list == [
        mock.call(make_message_for_next_step(message_1_1, False))
    ]

    next_step.reset_mock()

    # Partition 0, earlier timestamp so commit=True
    message_0_1 = Message(
        partition,
        4,
        Tick(
            0,
            offsets=Interval(2, 4),
            timestamps=Interval(
                epoch + timedelta(seconds=2), epoch + timedelta(seconds=5)
            ),
        ),
        epoch,
        5,
    )

    strategy.submit(message_0_1)

    assert next_step.submit.call_args_list == [
        mock.call(make_message_for_next_step(message_0_1, True))
    ]


def test_tick_buffer_with_commit_strategy() -> None:
    epoch = datetime(1970, 1, 1)
    now = datetime.now()

    metrics_backend = TestingMetricsBackend()

    next_step = mock.Mock()

    strategy = TickBuffer(
        SchedulingWatermarkMode.GLOBAL,
        2,
        10,
        ProvideCommitStrategy(2, next_step),
        metrics_backend,
    )

    topic = Topic("messages")
    commit_log_partition = Partition(topic, 0)

    # First message in partition 0, not submitted to next step
    message_0_0 = Message(
        commit_log_partition,
        4,
        Tick(
            0,
            offsets=Interval(1, 3),
            timestamps=Interval(epoch, epoch + timedelta(seconds=4)),
        ),
        now,
        5,
    )
    strategy.submit(message_0_0)

    assert next_step.submit.call_count == 0
    assert metrics_backend.calls == []

    # Another message in partition 0, cannot submit yet
    message_0_1 = Message(
        commit_log_partition,
        5,
        Tick(
            0,
            offsets=Interval(3, 6),
            timestamps=Interval(
                epoch + timedelta(seconds=4), epoch + timedelta(seconds=6)
            ),
        ),
        now,
        6,
    )
    strategy.submit(message_0_1)

    assert next_step.submit.call_count == 0
    assert metrics_backend.calls == []

    # Message in partition 1, submitted to next step since it has the earliest timestamp.
    # Does not commit since we have not submitted anything on the other partition yet.
    message_1_0 = Message(
        commit_log_partition,
        6,
        Tick(
            1,
            offsets=Interval(100, 120),
            timestamps=Interval(epoch, epoch + timedelta(seconds=3)),
        ),
        now,
        7,
    )
    strategy.submit(message_1_0)

    assert next_step.submit.call_count == 1
    assert next_step.submit.call_args_list == [
        mock.call(make_message_for_next_step(message_1_0, False)),
    ]

    next_step.reset_mock()

    # Another message in partition 1, now two more messages submitted
    # message_0_0 should be commited since all prior messages in the
    # commit log have been submitted (i.e. have a lower timestamp)
    # message_1_1 cannot be commited because message_0_1 is not submitted
    # yet (i.e. has a higher timestamp)
    message_1_1 = Message(
        commit_log_partition,
        7,
        Tick(
            1,
            offsets=Interval(120, 140),
            timestamps=Interval(
                epoch + timedelta(seconds=3), epoch + timedelta(seconds=4)
            ),
        ),
        now,
        8,
    )
    strategy.submit(message_1_1)

    assert next_step.submit.call_count == 2
    assert next_step.submit.call_args_list == [
        mock.call(make_message_for_next_step(message_0_0, True)),
        mock.call(make_message_for_next_step(message_1_1, False)),
    ]
