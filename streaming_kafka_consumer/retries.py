from abc import abstractmethod
from typing import Callable, Protocol, TypeVar

T = TypeVar("T")


class RetryPolicy(Protocol):
    """
    The retry policy defines an interface for invoking a callable until it
    passes some sort of test -- typically, that it evaluates without throwing
    an exception.
    """

    @abstractmethod
    def call(self, callable: Callable[[], T]) -> T:
        raise NotImplementedError


class NoRetryPolicy(RetryPolicy):
    """
    The no retry policy implements the retry policy interface, but does not
    retry. (This is equivalent to a basic retry policy with a limit of a
    single attempt.)
    """

    def call(self, callable: Callable[[], T]) -> T:
        return callable()
