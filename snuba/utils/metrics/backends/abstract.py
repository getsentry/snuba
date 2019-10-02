from abc import ABC, abstractmethod
from typing import Optional, Union

from snuba.utils.metrics import Tags


class MetricsBackend(ABC):
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
