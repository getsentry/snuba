from collections import ChainMap
from random import random
from typing import Mapping, Optional, Union

from snuba.utils.metrics.backends.abstract import MetricsBackend
from snuba.utils.metrics.types import Tags


class Metrics:
    """
    Manages the recording of metrics via the metrics backend.
    """

    def __init__(
        self,
        backend: MetricsBackend,
        namespace: Optional[str] = None,
        tags: Optional[Tags] = None,
        sample_rates: Optional[Mapping[str, float]] = None,
    ) -> None:
        """
        :param namespace: An optional prefix for all metrics recorded. This
        prefix will be joined with the metric name with the dot operator,
        e.g. ``prefix.metric``.
        :param tags: An optional collection of tags to include with all
        metrics recorded. If a tag key is defined in this collection as well
        as the tags that are passed to the metric method (such as
        ``increment``), the tag passed to the metric method will take
        precedence.
        :param sample_rates: An optional mapping of metric names to sample
        rates to use when recording metrics. A sample rate of ``0.0`` will
        disable a metric entirely, while a sample rate of ``1.0`` will cause
        all values for that metric to be recorded. To set a default metric
        rate, use a mapping type with a default value (such as
        ``collections.defaultdict``.)
        """
        self.__backend = backend
        self.__namespace = namespace
        self.__tags = tags if tags is not None else {}
        self.__sample_rates = sample_rates if sample_rates is not None else {}

    def __apply_namespace(self, name: str) -> str:
        if self.__namespace is not None:
            return f"{self.__namespace}.{name}"
        else:
            return name

    def __merge_tags(self, tags: Optional[Tags]) -> Tags:
        return ChainMap(tags or {}, self.__tags)

    def __is_sampled(self, name: str) -> bool:
        """
        Returns whether or not a metric should be recorded (e.g. is part of
        the population sample.)
        """
        return self.__sample_rates.get(name, 1.0) >= random()

    def increment(
        self, name: str, value: Union[int, float] = 1, tags: Optional[Tags] = None
    ) -> None:
        """
        Increment a counter.
        """
        if self.__is_sampled(name):
            self.__backend.increment(
                self.__apply_namespace(name), value, self.__merge_tags(tags)
            )

    def gauge(
        self, name: str, value: Union[int, float], tags: Optional[Tags] = None
    ) -> None:
        """
        Set the value of a gauge.
        """
        if self.__is_sampled(name):
            self.__backend.gauge(
                self.__apply_namespace(name), value, self.__merge_tags(tags)
            )

    def timing(
        self, name: str, value: Union[int, float], tags: Optional[Tags] = None
    ) -> None:
        """
        Record a timing. The unit of the value should be in milliseconds.
        """
        if self.__is_sampled(name):
            self.__backend.timing(
                self.__apply_namespace(name), value, self.__merge_tags(tags)
            )
