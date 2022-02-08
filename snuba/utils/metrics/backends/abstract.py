from abc import ABC, abstractmethod
from typing import Optional, Union

from snuba.utils.metrics.types import Tags


class MetricsBackend(ABC):
    """
    An abstract class that defines the interface for metrics backends.
    """

    @abstractmethod
    def increment(
        self, name: str, value: Union[int, float] = 1, tags: Optional[Tags] = None
    ) -> None:
        """
        Emit a single instance of a non-timing metric that can be counted,
        sliced/diced into percentiles, or summed
        """
        raise NotImplementedError

    @abstractmethod
    def gauge(
        self, name: str, value: Union[int, float], tags: Optional[Tags] = None
    ) -> None:
        """
        Emit a metric that is the authoritative value at a point in time
        """
        raise NotImplementedError

    @abstractmethod
    def timing(
        self, name: str, value: Union[int, float], tags: Optional[Tags] = None
    ) -> None:
        """
        Emit a metric for the performance of an operation
        """
        raise NotImplementedError
