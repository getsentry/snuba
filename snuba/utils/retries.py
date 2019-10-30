from abc import ABC, abstractmethod
from typing import Callable, Optional, TypeVar

from snuba.utils.clock import Clock, SystemClock


T = TypeVar("T")


def constant_delay(delay: float) -> Callable[[int], float]:
    def f(attempts: int) -> float:
        return delay
    return f


class RetryException(Exception):
    pass


class RetryPolicy(ABC):
    @abstractmethod
    def call(self, callable: Callable[[], T]) -> T:
        raise NotImplementedError


class NoRetryPolicy(RetryPolicy):
    def call(self, callable: Callable[[], T]) -> T:
        return callable()


class BasicRetryPolicy(RetryPolicy):
    def __init__(
        self,
        attempts: int,
        delay_function: Optional[Callable[[int], float]] = None,
        clock: Clock = SystemClock(),
    ) -> None:
        self.__attempts = attempts
        self.__delay_function = delay_function
        self.__clock = clock

    def call(self, callable: Callable[[], T]) -> T:
        for i in range(1, self.__attempts + 1):
            try:
                return callable()
            except Exception as exception:
                if i == self.__attempts:
                    raise RetryException() from exception

            if self.__delay_function is not None:
                self.__clock.sleep(self.__delay_function(i))

        raise RetryException
