import time
from abc import ABC, abstractmethod


class Clock(ABC):
    """
    An abstract clock interface.
    """

    @abstractmethod
    def time(self) -> float:
        raise NotImplementedError

    @abstractmethod
    def sleep(self, duration: float) -> None:
        raise NotImplementedError


class SystemClock(Clock):
    """
    A clock implementation that uses the system clock for the current time.
    """

    def time(self) -> float:
        return time.time()

    def sleep(self, duration: float) -> None:
        time.sleep(duration)


class TestingClock(Clock):
    """
    A clock implementation that uses a stable time for testing. To advance
    the time, use the ``sleep`` method.
    """

    def __init__(self, epoch: float = 0.0) -> None:
        self.__time = epoch

    def time(self) -> float:
        return self.__time

    def sleep(self, duration: float) -> None:
        self.__time = self.__time + duration
