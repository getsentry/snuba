import pytest
from snuba.utils.streams.parallel import OffsetTracker


@pytest.mark.parametrize("epoch", [0, 100])
def test_offset_tracking(epoch: int) -> None:
    tracker = OffsetTracker()
    assert tracker.value() is None
    assert len(tracker) == 0
    assert [*tracker] == []

    for i in range(3):
        tracker.add(epoch + i)
        assert tracker.value() == epoch
        assert len(tracker) == i + 1
        assert [*tracker] == [epoch + j for j in range(i + 1)]

    # Ensure the offset may only move monotonically.
    with pytest.raises(ValueError):
        tracker.add(epoch)

    tracker.add(epoch + 5)
    assert tracker.value() == epoch
    assert len(tracker) == 4
    assert [*tracker] == [epoch + i for i in [0, 1, 2, 5]]

    tracker.remove(epoch + 1)
    assert tracker.value() == epoch
    assert len(tracker) == 3
    assert [*tracker] == [epoch + i for i in [0, 2, 5]]

    tracker.remove(epoch)
    assert tracker.value() == epoch + 2
    assert len(tracker) == 2
    assert [*tracker] == [epoch + i for i in [2, 5]]

    # Ensure that offsets cannot be removed more than once.
    with pytest.raises(KeyError):
        tracker.remove(epoch)

    tracker.remove(epoch + 2)
    assert tracker.value() == epoch + 5
    assert len(tracker) == 1
    assert [*tracker] == [epoch + 5]

    tracker.remove(epoch + 5)
    assert tracker.value() == epoch + 6
    assert len(tracker) == 0
    assert [*tracker] == []

    # Ensure that only offsets that have been added can be removed.
    with pytest.raises(KeyError):
        tracker.remove(epoch + 4)  # skipped

    with pytest.raises(KeyError):
        tracker.remove(epoch - 1)  # out of range (too low)

    with pytest.raises(KeyError):
        tracker.remove(epoch + 100)  # out of range (too high)
