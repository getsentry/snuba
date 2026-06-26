from collections.abc import MutableSequence
from typing import NamedTuple

from snuba.utils.metrics.backends.abstract import MetricsBackend
from snuba.utils.metrics.types import Tags


class Increment(NamedTuple):
    name: str
    value: int | float
    tags: Tags | None
    unit: str | None = None


class Gauge(NamedTuple):
    name: str
    value: int | float
    tags: Tags | None
    unit: str | None = None


class Timing(NamedTuple):
    name: str
    value: int | float
    tags: Tags | None
    unit: str | None = None


class Distribution(NamedTuple):
    name: str
    value: int | float
    tags: Tags | None
    unit: str | None = None


class Events(NamedTuple):
    title: str
    text: str
    alert_type: str
    priority: str
    tags: Tags | None


class TestingMetricsBackend(MetricsBackend):
    """
    A metrics backend that logs all metrics recorded. Intended for testing
    the behavior of the metrics implementations themselves. Not for general
    use (it will cause unbounded memory consumption.)
    """

    # TODO: This might make sense to extend the dummy metrics backend.

    def __init__(self) -> None:
        self.calls: MutableSequence[Increment | Gauge | Timing | Distribution | Events] = []

    def increment(
        self,
        name: str,
        value: int | float = 1,
        tags: Tags | None = None,
        unit: str | None = None,
    ) -> None:
        self.calls.append(Increment(name, value, tags, unit))

    def gauge(
        self,
        name: str,
        value: int | float,
        tags: Tags | None = None,
        unit: str | None = None,
    ) -> None:
        self.calls.append(Gauge(name, value, tags, unit))

    def timing(
        self,
        name: str,
        value: int | float,
        tags: Tags | None = None,
        unit: str | None = None,
    ) -> None:
        self.calls.append(Timing(name, value, tags, unit))

    def distribution(
        self,
        name: str,
        value: int | float,
        tags: Tags | None = None,
        unit: str | None = None,
    ) -> None:
        self.calls.append(Distribution(name, value, tags, unit))

    def events(
        self,
        title: str,
        text: str,
        alert_type: str,
        priority: str,
        tags: Tags | None = None,
    ) -> None:
        self.calls.append(Events(title, text, alert_type, priority, tags))
