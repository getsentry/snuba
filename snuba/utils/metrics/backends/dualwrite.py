from __future__ import annotations

import random

from snuba import settings
from snuba.utils.metrics.backends.abstract import MetricsBackend
from snuba.utils.metrics.backends.datadog import DatadogMetricsBackend
from snuba.utils.metrics.backends.sentry import SentryMetricsBackend
from snuba.utils.metrics.types import Tags


class SentryDatadogMetricsBackend(MetricsBackend):
    """
    A metrics backend that records metrics to Sentry and Datadog.
    """

    def __init__(
        self, datadog: DatadogMetricsBackend, sentry: SentryMetricsBackend
    ) -> None:
        self.datadog = datadog
        self.sentry = sentry

    def _use_sentry(self) -> bool:
        from snuba import state

        if str(state.get_config("use_sentry_metrics", "0")) == "1":
            return bool(random.random() < settings.DDM_METRICS_SAMPLE_RATE)
        return False

    def increment(
        self,
        name: str,
        value: int | float = 1,
        tags: Tags | None = None,
        unit: str | None = None,
    ) -> None:
        self.datadog.increment(name, value, tags, unit)
        if self._use_sentry():
            self.sentry.increment(name, value, tags, unit)

    def gauge(
        self,
        name: str,
        value: int | float,
        tags: Tags | None = None,
        unit: str | None = None,
    ) -> None:
        self.datadog.gauge(name, value, tags, unit)
        if self._use_sentry():
            self.sentry.gauge(name, value, tags, unit)

    def timing(
        self,
        name: str,
        value: int | float,
        tags: Tags | None = None,
        unit: str | None = None,
    ) -> None:
        self.datadog.timing(name, value, tags, unit)
        if self._use_sentry():
            self.sentry.timing(name, value, tags, unit)

    def distribution(
        self,
        name: str,
        value: int | float,
        tags: Tags | None = None,
        unit: str | None = None,
    ) -> None:
        self.datadog.distribution(name, value, tags, unit)
        if self._use_sentry():
            self.sentry.distribution(name, value, tags, unit)

    def events(
        self,
        title: str,
        text: str,
        alert_type: str,
        priority: str,
        tags: Tags | None = None,
    ) -> None:
        self.datadog.events(title, text, alert_type, priority, tags)
