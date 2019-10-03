import time
from abc import ABC, abstractmethod


class Clock(ABC):
    @abstractmethod
    def time(self) -> float:
        raise NotImplementedError

    @abstractmethod
    def sleep(self, duration: float) -> None:
        raise NotImplementedError


class SystemClock(Clock):
    def time(self) -> float:
        return time.time()

    def sleep(self, duration: float) -> None:
        time.sleep(duration)


class TestingClock(Clock):
    def __init__(self) -> None:
        self.__time = 0.0

    def time(self) -> float:
        return self.__time

    def sleep(self, duration: float) -> None:
        self.__time = self.__time + duration
