from __future__ import annotations

import logging
import threading
from typing import Callable, Mapping, Optional, Sequence, Union

from datadog import DogStatsd

from snuba.utils.metrics.backends.abstract import MetricsBackend
from snuba.utils.metrics.types import Tags

logger = logging.getLogger(__name__)


class DatadogMetricsBackend(MetricsBackend):
    """
    A metrics backend that records metrics to Datadog.
    """

    def __init__(
        self,
        client_factory: Callable[[], DogStatsd],
        sample_rates: Optional[Mapping[str, float]] = None,
    ) -> None:
        """
        :param client_factory: A function that returns a new ``DogStatsd``
        instance. (These instances are not thread safe, so a new instance
        will be created for each independent thread.)
        :param sample_rates: An optional mapping of metric names to sample
        rates to use when recording metrics. A sample rate of ``0.0`` will
        disable a metric entirely, while a sample rate of ``1.0`` will cause
        all values for that metric to be recorded.
        """
        self.__client_factory = client_factory
        self.__sample_rates = sample_rates if sample_rates is not None else {}
        self.__thread_state = threading.local()

    @property
    def __client(self) -> Optional[DogStatsd]:
        try:
            client = self.__thread_state.client
        except AttributeError:
            try:
                client = self.__thread_state.client = self.__client_factory()
            except Exception as e:
                # During thread cleanup (e.g., ThreadPoolExecutor shutdown), the client
                # factory may fail or set an exception while returning a value, causing
                # a SystemError. We catch this and log it, returning None to allow
                # metrics calls to fail silently rather than crashing the application.
                logger.warning(
                    "Failed to create DogStatsd client in thread %s: %s",
                    threading.current_thread().name,
                    e,
                    exc_info=True,
                )
                return None
        return client

    def __normalize_tags(self, tags: Optional[Tags]) -> Optional[Sequence[str]]:
        if tags is None:
            return None
        else:
            return [f"{key}:{value.replace('|', '_')}" for key, value in tags.items()]

    def increment(
        self,
        name: str,
        value: Union[int, float] = 1,
        tags: Optional[Tags] = None,
        unit: Optional[str] = None,
    ) -> None:
        client = self.__client
        if client is None:
            return
        client.increment(
            name,
            value,
            tags=self.__normalize_tags(tags),
            sample_rate=self.__sample_rates.get(name, 1.0),
        )

    def gauge(
        self,
        name: str,
        value: Union[int, float],
        tags: Optional[Tags] = None,
        unit: Optional[str] = None,
    ) -> None:
        client = self.__client
        if client is None:
            return
        client.gauge(
            name,
            value,
            tags=self.__normalize_tags(tags),
            sample_rate=self.__sample_rates.get(name, 1.0),
        )

    def timing(
        self,
        name: str,
        value: Union[int, float],
        tags: Optional[Tags] = None,
        unit: Optional[str] = None,
    ) -> None:
        client = self.__client
        if client is None:
            return
        client.timing(
            name,
            value,
            tags=self.__normalize_tags(tags),
            sample_rate=self.__sample_rates.get(name, 1.0),
        )

    def distribution(
        self,
        name: str,
        value: Union[int, float],
        tags: Optional[Tags] = None,
        unit: Optional[str] = None,
    ) -> None:
        client = self.__client
        if client is None:
            return
        client.distribution(
            name,
            value,
            tags=self.__normalize_tags(tags),
            sample_rate=self.__sample_rates.get(name, 1.0),
        )

    def events(
        self,
        title: str,
        text: str,
        alert_type: str,
        priority: str,
        tags: Optional[Tags] = None,
    ) -> None:
        client = self.__client
        if client is None:
            return
        client.event(
            title=title,
            text=text,
            alert_type=alert_type,
            tags=self.__normalize_tags(tags),
            priority=priority,
        )
