from typing import Optional, Mapping, Sequence, Union

from datadog import DogStatsd

from snuba.utils.metrics.backends.abstract import MetricsBackend
from snuba.utils.metrics.types import Tags


class DatadogMetricsBackend(MetricsBackend):
    """
    A metrics backend that records metrics to Datadog.
    """

    def __init__(
        self, client: DogStatsd, sample_rates: Optional[Mapping[str, float]] = None
    ) -> None:
        """
        :param sample_rates: An optional mapping of metric names to sample
        rates to use when recording metrics. A sample rate of ``0.0`` will
        disable a metric entirely, while a sample rate of ``1.0`` will cause
        all values for that metric to be recorded.
        """
        self.__client = client
        self.__sample_rates = sample_rates if sample_rates is not None else {}

    def __normalize_tags(self, tags: Optional[Tags]) -> Optional[Sequence[str]]:
        if tags is None:
            return None
        else:
            return [f"{key}:{value}" for key, value in tags.items()]

    def increment(
        self, name: str, value: Union[int, float] = 1, tags: Optional[Tags] = None
    ) -> None:
        self.__client.increment(
            name,
            value,
            tags=self.__normalize_tags(tags),
            sample_rate=self.__sample_rates.get(name, 1.0),
        )

    def gauge(
        self, name: str, value: Union[int, float], tags: Optional[Tags] = None
    ) -> None:
        self.__client.gauge(
            name,
            value,
            tags=self.__normalize_tags(tags),
            sample_rate=self.__sample_rates.get(name, 1.0),
        )

    def timing(
        self, name: str, value: Union[int, float], tags: Optional[Tags] = None
    ) -> None:
        self.__client.timing(
            name,
            value,
            tags=self.__normalize_tags(tags),
            sample_rate=self.__sample_rates.get(name, 1.0),
        )
