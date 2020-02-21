from threading import Lock
from typing import Any, Optional

from snuba.utils.metrics.backends.abstract import MetricsBackend
from snuba.utils.metrics.types import Tags


class Gauge:
    """
    Implements a thread-safe gauge as a context manager. This can be used to
    track how many threads are concurrently executing a code block.
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

        self.__metrics = metrics
        self.__name = name
        self.__tags = tags

        self.__lock = lock
        self.__value = 0

        self.__report()

    def __report(self) -> None:
        self.__metrics.gauge(self.__name, self.__value, self.__tags)

    def __enter__(self) -> None:
        with self.__lock:
            self.__value += 1
            self.__report()

    def __exit__(
        self,
        type: Optional[Any] = None,
        value: Optional[Any] = None,
        traceback: Optional[Any] = None,
    ) -> None:
        with self.__lock:
            self.__value -= 1
            self.__report()
