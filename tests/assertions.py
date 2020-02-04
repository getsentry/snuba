import operator
from contextlib import contextmanager
from typing import Callable, Iterator, TypeVar

T = TypeVar("T")


@contextmanager
def assert_changes(
    callable: Callable[[], T],
    before: T,
    after: T,
    operator: Callable[[T, T], bool] = operator.eq,
) -> Iterator[None]:
    assert operator(callable(), before)
    yield
    assert operator(callable(), after)


@contextmanager
def assert_does_not_change(
    callable: Callable[[], T], value: T, operator: Callable[[T, T], bool] = operator.eq,
) -> Iterator[None]:
    assert operator(callable(), value)
    yield
    assert operator(callable(), value)
