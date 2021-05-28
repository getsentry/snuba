from abc import ABC, abstractmethod
from typing import Callable, Optional, TypeVar, Union

from snuba.utils.clock import Clock, SystemClock

T = TypeVar("T")


def constant_delay(delay: float) -> Callable[[int], float]:
    def f(attempts: int) -> float:
        return delay

    return f


class RetryException(Exception):
    pass


class RetryPolicy(ABC):
    """
    The retry policy defines an interface for invoking a callable until it
    passes some sort of test -- typically, that it evaluates without throwing
    an exception.
    """

    @abstractmethod
    def call(self, callable: Callable[[], T]) -> T:
        raise NotImplementedError


class BasicRetryPolicy(RetryPolicy):
    """
    The basic retry policy attempts to invoke the provided callable up to a
    specific number of attempts. If the callable cannot be successfully
    invoked in the defined number of attempts, a ``RetryException`` will be
    raised.

    The amount of time between retries can be controlled by providing a
    ``delay`` argument: which can be either a constant numeric value or a
    function returning a numeric value that represents the amount of time to
    wait between attempts. If no ``delay`` argument is provided, the callable
    will be retried immediately.

    By default, all exceptions that inherit from ``Exception`` (not including
    ``BaseException``) are caught and supressed, leading to a retry attempt.
    If the callable should only be retried under a limited set of
    circumstances, a suppression test function can be provided, which
    recieves the exception instance as its sole argument. If the suppression
    test returns ``True``, the exception will be suppressed and the callable
    will be retried (as long as the number of attempts remaining permits.) If
    the suppression test returns ``False``, the exception will be raised
    immediately.
    """

    def __init__(
        self,
        attempts: int,
        delay: Union[None, float, Callable[[int], float]] = None,
        suppression_test: Optional[Callable[[Exception], bool]] = None,
        clock: Clock = SystemClock(),
    ) -> None:
        delay_function: Optional[Callable[[int], float]] = None
        if isinstance(delay, (int, float)):
            delay_function = constant_delay(delay)
        else:
            delay_function = delay

        self.__attempts = attempts
        self.__delay_function = delay_function
        self.__suppression_test = suppression_test
        self.__clock = clock

    def call(self, callable: Callable[[], T]) -> T:
        for i in range(1, self.__attempts + 1):
            try:
                return callable()
            except Exception as exception:
                if self.__suppression_test is not None and not self.__suppression_test(
                    exception
                ):
                    raise

                if i == self.__attempts:
                    raise RetryException() from exception

            if self.__delay_function is not None:
                self.__clock.sleep(self.__delay_function(i))

        raise Exception("unexpected fallthrough")
