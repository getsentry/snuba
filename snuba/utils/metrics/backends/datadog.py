from typing import Optional, Sequence, Union

from datadog import DogStatsd

from snuba.utils.metrics.backends.abstract import MetricsBackend
from snuba.utils.metrics.types import Tags


class DatadogMetricsBackend(MetricsBackend):
    def __init__(self, client: DogStatsd):
        self.__client = client

    def __normalize_tags(self, tags: Optional[Tags]) -> Optional[Sequence[str]]:
        if tags is None:
            return None
        else:
            return [f"{key}:{value}" for key, value in tags.items()]

    def increment(
        self, name: str, value: Union[int, float] = 1, tags: Optional[Tags] = None
    ) -> None:
        self.__client.increment(name, value, tags=self.__normalize_tags(tags))

    def gauge(
        self, name: str, value: Union[int, float], tags: Optional[Tags] = None
    ) -> None:
        self.__client.gauge(name, value, tags=self.__normalize_tags(tags))

    def timing(
        self, name: str, value: Union[int, float], tags: Optional[Tags] = None
    ) -> None:
        self.__client.timing(name, value, tags=self.__normalize_tags(tags))
