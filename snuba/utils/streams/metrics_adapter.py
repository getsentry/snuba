from snuba.utils.metrics import MetricsBackend
from snuba.utils.metrics.types import Tags


class StreamMetricsAdapter:
    def __init__(self, metrics: MetricsBackend) -> None:
        self.__wrapper = metrics

    def increment(self, name: str, value: int | float = 1, tags: Tags | None = None) -> None:
        self.__wrapper.increment(name, value, tags)

    def gauge(self, name: str, value: int | float, tags: Tags | None = None) -> None:
        self.__wrapper.gauge(name, value, tags)

    def timing(self, name: str, value: int | float, tags: Tags | None = None) -> None:
        self.__wrapper.timing(name, value, tags)
