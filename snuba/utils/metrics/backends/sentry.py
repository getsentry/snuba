from __future__ import annotations

from typing import Any

from sentry_sdk import metrics

from snuba.utils.metrics.backends.abstract import MetricsBackend
from snuba.utils.metrics.types import Tags


def _attributes(tags: Tags | None) -> dict[str, Any] | None:
    """The SDK takes an invariant ``dict``; ``Tags`` is a ``Mapping``."""
    return dict(tags) if tags is not None else None


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
        metrics.count(name, value, unit or "none", _attributes(tags))

    def gauge(
        self,
        name: str,
        value: int | float,
        tags: Tags | None = None,
        unit: str | None = None,
    ) -> None:
        metrics.gauge(name, value, unit or "none", _attributes(tags))

    def timing(
        self,
        name: str,
        value: int | float,
        tags: Tags | None = None,
        unit: str | None = None,
    ) -> None:
        # The SDK dropped the dedicated `timing` API; a timing is a distribution
        # measured in milliseconds.
        metrics.distribution(name, value, unit or "millisecond", _attributes(tags))

    def distribution(
        self,
        name: str,
        value: int | float,
        tags: Tags | None = None,
        unit: str | None = None,
    ) -> None:
        metrics.distribution(name, value, unit or "none", _attributes(tags))

    def events(
        self,
        title: str,
        text: str,
        alert_type: str,
        priority: str,
        tags: Tags | None = None,
    ) -> None:
        return None  # Sentry doesn't support events
