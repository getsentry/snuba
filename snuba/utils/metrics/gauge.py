from threading import Lock
from typing import Any, Optional

from snuba.utils.metrics.backends.abstract import MetricsBackend
from snuba.utils.metrics.types import Tags


class Gauge:
    def __init__(
        self, metrics: MetricsBackend, name: str, tags: Optional[Tags] = None,
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


class ThreadSafeGauge(Gauge):
    """
    Implements a thread-safe gauge.

    This can be used to track how many threads are concurrently executing a
    code block.
    """

    def __init__(
        self,
        metrics: MetricsBackend,
        name: str,
        tags: Optional[Tags] = None,
        lock: Optional[Lock] = None,
    ) -> None:
        if lock is None:
            lock = Lock()

        super().__init__(metrics, name, tags)

        self.__lock = lock

    def increment(self, value: float = 1.0) -> None:
        with self.__lock:
            super().increment(value)

    def decrement(self, value: float = 1.0) -> None:
        with self.__lock:
            super().decrement(value)
