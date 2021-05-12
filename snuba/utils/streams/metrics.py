from abc import ABC, abstractmethod
from typing import Any, Mapping, Optional, Union

Tags = Optional[Mapping[str, str]]


class Metrics(ABC):
    """
    An abstract class that defines the interface for metrics backends.
    """

    @abstractmethod
    def increment(
        self, name: str, value: Union[int, float] = 1, tags: Optional[Tags] = None
    ) -> None:
        raise NotImplementedError

    @abstractmethod
    def gauge(
        self, name: str, value: Union[int, float], tags: Optional[Tags] = None
    ) -> None:
        raise NotImplementedError

    @abstractmethod
    def timing(
        self, name: str, value: Union[int, float], tags: Optional[Tags] = None
    ) -> None:
        raise NotImplementedError


class _DummyMetricsBackend(Metrics):
    """
    Default metrics backend that does not record anything.
    """

    def increment(
        self, name: str, value: Union[int, float] = 1, tags: Optional[Tags] = None
    ) -> None:
        pass

    def gauge(
        self, name: str, value: Union[int, float], tags: Optional[Tags] = None
    ) -> None:
        pass

    def timing(
        self, name: str, value: Union[int, float], tags: Optional[Tags] = None
    ) -> None:
        pass


DummyMetricsBackend = _DummyMetricsBackend()


class Gauge:
    def __init__(
        self, metrics: Metrics, name: str, tags: Optional[Tags] = None,
    ) -> None:
        self.__metrics = metrics
        self.__name = name
        self.__tags = tags

        self.__value = 0.0

        self.__report()

    def __enter__(self) -> None:
        self.increment()

    def __exit__(
        self,
        type: Optional[Any] = None,
        value: Optional[Any] = None,
        traceback: Optional[Any] = None,
    ) -> None:
        self.decrement()

    def __report(self) -> None:
        self.__metrics.gauge(self.__name, self.__value, self.__tags)

    def increment(self, value: float = 1.0) -> None:
        self.__value += value
        self.__report()

    def decrement(self, value: float = 1.0) -> None:
        self.__value -= value
        self.__report()
