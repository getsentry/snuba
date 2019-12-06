import pytest

from snuba.utils.types import Interval


def test_interval_validation() -> None:
    Interval(1, 1)
    Interval(1, 10)
    with pytest.raises(AssertionError):
        Interval(10, 1)
