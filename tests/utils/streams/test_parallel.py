import pytest
from snuba.utils.streams.parallel import OffsetTracker


@pytest.mark.parametrize("epoch", [0, 100])
def test_offset_tracking(epoch: int) -> None:
    tracker = OffsetTracker(epoch)
    assert tracker.value() == epoch

    for i in range(3):
        tracker.add(epoch + i)
        assert tracker.value() == epoch

    # Ensure the offset may only move monotonically.
    with pytest.raises(ValueError):
        tracker.add(epoch)

    tracker.add(epoch + 5)
    assert tracker.value() == epoch

    tracker.remove(epoch + 1)
    assert tracker.value() == epoch

    tracker.remove(epoch)
    assert tracker.value() == epoch + 2

    # Ensure that offsets cannot be removed more than once.
    with pytest.raises(ValueError):
        tracker.remove(epoch)

    tracker.remove(epoch + 2)
    assert tracker.value() == epoch + 5

    tracker.remove(epoch + 5)
    assert tracker.value() == epoch + 6

    # Ensure that only offsets that have been added can be removed.
    with pytest.raises(ValueError):
        tracker.remove(epoch + 4)  # skipped

    with pytest.raises(ValueError):
        tracker.remove(epoch - 1)  # out of range (too low)

    with pytest.raises(ValueError):
        tracker.remove(epoch + 100)  # out of range (too high)
