from typing import MutableSequence, NamedTuple, Optional, Union

from snuba.utils.metrics.backends.abstract import MetricsBackend
from snuba.utils.metrics.types import Tags


class Increment(NamedTuple):
    name: str
    value: Union[int, float]
    tags: Optional[Tags]


class Gauge(NamedTuple):
    name: str
    value: Union[int, float]
    tags: Optional[Tags]


class Timing(NamedTuple):
    name: str
    value: Union[int, float]
    tags: Optional[Tags]


# TODO: This might make sense to extend the dummy metrics backend.
class TestingMetricsBackend(MetricsBackend):
    def __init__(self) -> None:
        self.calls: MutableSequence[Union[Increment, Gauge, Timing]] = []

    def increment(
        self, name: str, value: Union[int, float] = 1, tags: Optional[Tags] = None
    ) -> None:
        self.calls.append(Increment(name, value, tags))

    def gauge(
        self, name: str, value: Union[int, float], tags: Optional[Tags] = None
    ) -> None:
        self.calls.append(Gauge(name, value, tags))

    def timing(
        self, name: str, value: Union[int, float], tags: Optional[Tags] = None
    ) -> None:
        self.calls.append(Timing(name, value, tags))
