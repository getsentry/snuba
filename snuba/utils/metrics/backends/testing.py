from __future__ import annotations

from collections.abc import Mapping, MutableMapping
from dataclasses import dataclass

from snuba.utils.metrics.backends.abstract import MetricsBackend
from snuba.utils.metrics.types import Tags


@dataclass(frozen=True)
class RecordedMetricCall:
    value: int | float
    tags: Tags
    unit: str | None


@dataclass(frozen=True)
class RecordedEventCall:
    value: str
    tags: Tags


RecordedMetricCalls = list[RecordedMetricCall]


RECORDED_METRIC_CALLS: MutableMapping[str, MutableMapping[str, list[RecordedMetricCall]]] = {}
RECORDED_EVENT_CALLS: MutableMapping[str, list[RecordedEventCall]] = {}


def record_metric_call(
    mtype: str,
    name: str,
    value: int | float,
    tags: Tags | None,
    unit: str | None = None,
) -> None:
    if mtype not in RECORDED_METRIC_CALLS:
        RECORDED_METRIC_CALLS[mtype] = {}

    if name not in RECORDED_METRIC_CALLS[mtype]:
        RECORDED_METRIC_CALLS[mtype][name] = []

    if tags is None:
        tags = {}
    RECORDED_METRIC_CALLS[mtype][name].append(RecordedMetricCall(value, tags, unit))


def record_event_call(
    title: str, text: str, alert_type: str, priority: str, tags: Tags | None = None
) -> None:
    value = str(
        {
            "title": title,
            "text": text,
            "alert_type": alert_type,
            "priority": priority,
        }
    )
    RECORDED_EVENT_CALLS[title].append(RecordedEventCall(value, tags or {}))


def clear_recorded_metric_calls() -> None:
    global RECORDED_METRIC_CALLS
    RECORDED_METRIC_CALLS = {}
    RECORDED_EVENT_CALLS.clear()


def get_recorded_metric_calls(mtype: str, name: str) -> RecordedMetricCalls | None:
    """
    Used in tests to determine if the metrics were called with the correct values
    """
    return RECORDED_METRIC_CALLS.get(mtype, {}).get(name)


class TestingMetricsBackend(MetricsBackend):
    """
    A metrics backend that records metrics locally, to be verified in tests.
    """

    def __init__(self, strict: bool = False):
        """
        :param strict: Enable runtime type checking of parameter values.
        """
        self.__strict = strict

    def __validate_tags(self, tags: Tags) -> None:
        assert isinstance(tags, Mapping)
        for k, v in tags.items():
            assert isinstance(k, str)
            assert isinstance(v, str)

    def increment(
        self,
        name: str,
        value: int | float = 1,
        tags: Tags | None = None,
        unit: str | None = None,
    ) -> None:
        record_metric_call("increment", name, value, tags)
        if self.__strict:
            assert isinstance(name, str)
            assert isinstance(value, (int, float))
            if tags is not None:
                self.__validate_tags(tags)

    def gauge(
        self,
        name: str,
        value: int | float,
        tags: Tags | None = None,
        unit: str | None = None,
    ) -> None:
        record_metric_call("gauge", name, value, tags)
        if self.__strict:
            assert isinstance(name, str)
            assert isinstance(value, (int, float))
            if tags is not None:
                self.__validate_tags(tags)

    def timing(
        self,
        name: str,
        value: int | float,
        tags: Tags | None = None,
        unit: str | None = None,
    ) -> None:
        record_metric_call("timing", name, value, tags)
        if self.__strict:
            assert isinstance(name, str)
            assert isinstance(value, (int, float))
            if tags is not None:
                self.__validate_tags(tags)

    def distribution(
        self,
        name: str,
        value: int | float,
        tags: Tags | None = None,
        unit: str | None = None,
    ) -> None:
        record_metric_call("distribution", name, value, tags)
        if self.__strict:
            assert isinstance(name, str)
            assert isinstance(value, (int, float))
            if tags is not None:
                self.__validate_tags(tags)

    def events(
        self,
        title: str,
        text: str,
        alert_type: str,
        priority: str,
        tags: Tags | None = None,
    ) -> None:
        record_event_call(title, text, alert_type, priority, tags)
        if self.__strict:
            assert isinstance(title, str)
            assert isinstance(text, str)
            assert isinstance(alert_type, str)
            assert isinstance(priority, str)
            if tags is not None:
                self.__validate_tags(tags)
