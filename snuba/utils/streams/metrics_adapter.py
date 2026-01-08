from typing import Optional, Union

from snuba.utils.metrics import MetricsBackend
from snuba.utils.metrics.types import Tags


class StreamMetricsAdapter:
    def __init__(self, metrics: MetricsBackend) -> None:
        self.__wrapper = metrics

    def increment(
        self, name: str, value: Union[int, float] = 1, tags: Optional[Tags] = None
    ) -> None:
        self.__wrapper.increment(name, value, tags)

    def gauge(self, name: str, value: Union[int, float], tags: Optional[Tags] = None) -> None:
        self.__wrapper.gauge(name, value, tags)

    def timing(self, name: str, value: Union[int, float], tags: Optional[Tags] = None) -> None:
        self.__wrapper.timing(name, value, tags)
