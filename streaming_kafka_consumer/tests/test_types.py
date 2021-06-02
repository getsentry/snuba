import pytest

from snuba.utils.types import Interval, InvalidRangeError


def test_interval_validation() -> None:
    Interval(1, 1)
    Interval(1, 10)

    with pytest.raises(InvalidRangeError) as e:
        Interval(10, 1)

    assert e.value.lower == 10
    assert e.value.upper == 1
