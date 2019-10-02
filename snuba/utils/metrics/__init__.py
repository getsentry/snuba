from random import random
from collections import ChainMap
from typing import Mapping, Optional, Union
from snuba.utils.metrics.backends.abstract import MetricsBackend


Tags = Mapping[str, str]


class Metrics:
    def __init__(
        self,
        backend: MetricsBackend,
        namespace: Optional[str] = None,
        tags: Optional[Tags] = None,
        sample_rates: Optional[Mapping[str, float]] = None,
    ) -> None:
        self.__backend = backend
        self.__namespace = namespace
        self.__tags = tags
        self.__sample_rates = sample_rates if sample_rates is not None else {}

    def __apply_namespace(self, name: str) -> str:
        return f"{self.__namespace}.{name}"

    def __merge_tags(self, tags: Optional[Tags]) -> Tags:
        return ChainMap(*[d for d in [self.__tags, tags] if d is not None])

    def __is_sampled(self, name: str) -> bool:
        return self.__sample_rates.get(name, 1.0) >= random()

    def increment(
        self, name: str, value: Union[int, float] = 1, tags: Optional[Tags] = None
    ) -> None:
        if self.__is_sampled(name):
            self.__backend.increment(
                self.__apply_namespace(name), value, self.__merge_tags(tags)
            )

    def gauge(
        self, name: str, value: Union[int, float], tags: Optional[Tags] = None
    ) -> None:
        if self.__is_sampled(name):
            self.__backend.gauge(
                self.__apply_namespace(name), value, self.__merge_tags(tags)
            )

    def timing(
        self, name: str, value: Union[int, float], tags: Optional[Tags] = None
    ) -> None:
        if self.__is_sampled(name):
            self.__backend.timing(
                self.__apply_namespace(name), value, self.__merge_tags(tags)
            )
