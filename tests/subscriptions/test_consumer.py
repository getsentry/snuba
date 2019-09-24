import pytest
import time

from snuba.subscriptions.consumer import Interval, PartitionState, Position


def test_interval():
    now = time.time()

    with pytest.raises(ValueError):
        Interval(
            lower=Position(offset=0, timestamp=now),
            upper=Position(offset=1, timestamp=now - 1),
        )

    with pytest.raises(ValueError):
        Interval(
            lower=Position(offset=0, timestamp=now),
            upper=Position(offset=0, timestamp=now),
        )

    interval = Interval(
        lower=Position(offset=0, timestamp=now), upper=Position(offset=1, timestamp=now)
    )

    interval = interval.shift(Position(offset=2, timestamp=now))

    with pytest.raises(ValueError):
        interval.shift(Position(offset=0, timestamp=now))


def test_partition_state():
    now = time.time()

    partition_state = PartitionState()

    assert partition_state.set_position(Position(offset=0, timestamp=now)) is None

    assert partition_state.set_position(
        Position(offset=1, timestamp=now + 1)
    ) == Interval(
        lower=Position(offset=0, timestamp=now),
        upper=Position(offset=1, timestamp=now + 1),
    )
