import pytest

from snuba.subscriptions.types import Interval, InvalidRangeError


def test_interval_validation() -> None:
    Interval(1, 1)
    Interval(1, 10)

    with pytest.raises(InvalidRangeError) as e:
        Interval(10, 1)

    with pytest.raises(InvalidRangeError):
        Interval(1, None)  # type: ignore

    assert e.value.lower == 10
    assert e.value.upper == 1
