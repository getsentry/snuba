from __future__ import annotations

from typing import Any, cast

from sentry_sdk import metrics

from snuba.utils.metrics.backends.abstract import MetricsBackend
from snuba.utils.metrics.types import Tags


def _attributes(tags: Tags | None) -> dict[str, Any] | None:
    """Cast ``Tags`` for the SDK's invariant ``dict`` annotation.

    The SDK only iterates ``.items()`` into a fresh dict, so a ``Mapping`` is
    fine at runtime and copying here would be pure waste.
    """
    return cast("dict[str, Any] | None", tags)


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
        # SDK dropped timing; emit as a millisecond distribution.
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
