from __future__ import annotations

from dataclasses import dataclass
from typing import List, Mapping, MutableMapping, Optional, Union

from snuba.utils.metrics.backends.abstract import MetricsBackend
from snuba.utils.metrics.types import Tags


@dataclass(frozen=True)
class RecordedMetricCall:
    value: int | float
    tags: Tags


RecordedMetricCalls = List[RecordedMetricCall]


RECORDED_METRIC_CALLS: MutableMapping[
    str, MutableMapping[str, List[RecordedMetricCall]]
] = {}


def record_metric_call(
    mtype: str, name: str, value: int | float, tags: Optional[Tags]
) -> None:
    if mtype not in RECORDED_METRIC_CALLS:
        RECORDED_METRIC_CALLS[mtype] = {}

    if name not in RECORDED_METRIC_CALLS[mtype]:
        RECORDED_METRIC_CALLS[mtype][name] = []

    if tags is None:
        tags = {}
    RECORDED_METRIC_CALLS[mtype][name].append(RecordedMetricCall(value, tags))


def clear_recorded_metric_calls() -> None:
    global RECORDED_METRIC_CALLS
    RECORDED_METRIC_CALLS = {}


def get_recorded_metric_calls(mtype: str, name: str) -> RecordedMetricCalls | None:
    """
    Used in tests to determine if the metrics were called with the correct values
    """
    return RECORDED_METRIC_CALLS.get(mtype, dict()).get(name)


class DummyMetricsBackend(MetricsBackend):
    """
    A metrics backend that does not record metrics. Intended for use during
    development, or other environments where metrics support may not be
    required.
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
        self, name: str, value: Union[int, float] = 1, tags: Optional[Tags] = None
    ) -> None:
        record_metric_call("increment", name, value, tags)
        if self.__strict:
            assert isinstance(name, str)
            assert isinstance(value, (int, float))
            if tags is not None:
                self.__validate_tags(tags)

    def gauge(
        self, name: str, value: Union[int, float], tags: Optional[Tags] = None
    ) -> None:
        record_metric_call("gauge", name, value, tags)
        if self.__strict:
            assert isinstance(name, str)
            assert isinstance(value, (int, float))
            if tags is not None:
                self.__validate_tags(tags)

    def timing(
        self, name: str, value: Union[int, float], tags: Optional[Tags] = None
    ) -> None:
        record_metric_call("timing", name, value, tags)
        if self.__strict:
            assert isinstance(name, str)
            assert isinstance(value, (int, float))
            if tags is not None:
                self.__validate_tags(tags)
