from __future__ import annotations

from sentry_sdk import metrics

from snuba.utils.metrics.backends.abstract import MetricsBackend
from snuba.utils.metrics.types import Tags


class SentryMetricsBackend(MetricsBackend):
    """
    A metrics backend that records metrics to Sentry.
    """

    def __init__(self) -> None:
        return None  # Sentry doesn't require any setup

    def increment(
        self,
        name: str,
        value: int | float = 1,
        tags: Tags | None = None,
        unit: str | None = None,
    ) -> None:
        metrics.incr(name, value, unit or "none", tags)

    def gauge(
        self,
        name: str,
        value: int | float,
        tags: Tags | None = None,
        unit: str | None = None,
    ) -> None:
        metrics.gauge(name, value, unit or "none", tags)

    def timing(
        self,
        name: str,
        value: int | float,
        tags: Tags | None = None,
        unit: metrics.DurationUnit | None = None,
    ) -> None:
        metrics.timing(name, value, unit or "millisecond", tags)

    def distribution(
        self,
        name: str,
        value: int | float,
        tags: Tags | None = None,
        unit: metrics.MeasurementUnit | None = None,
    ) -> None:
        metrics.distribution(name, value, unit or "none", tags)

    def events(
        self,
        title: str,
        text: str,
        alert_type: str,
        priority: str,
        tags: Tags | None = None,
    ) -> None:
        return None  # Sentry doesn't support events
