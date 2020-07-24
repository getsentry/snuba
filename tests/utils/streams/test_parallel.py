import pytest
from snuba.utils.streams.parallel import OffsetTracker


@pytest.mark.parametrize("epoch", [0, 100])
def test_offset_tracking(epoch: int) -> None:
    tracker = OffsetTracker(epoch)
    assert tracker.value() == epoch

    for i in range(3):
        tracker.add(epoch + i)
        assert tracker.value() == epoch

    tracker.remove(epoch + 1)
    assert tracker.value() == epoch

    tracker.remove(epoch)
    assert tracker.value() == epoch + 2

    tracker.remove(epoch + 2)
    assert tracker.value() == epoch + 3
