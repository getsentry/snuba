from collections import Mapping
from typing import Optional, Union

from snuba.utils.metrics.backends.abstract import MetricsBackend
from snuba.utils.metrics.types import Tags


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
        if self.__strict:
            assert isinstance(name, str)
            assert isinstance(value, (int, float))
            if tags is not None:
                self.__validate_tags(tags)

    def gauge(
        self, name: str, value: Union[int, float], tags: Optional[Tags] = None
    ) -> None:
        if self.__strict:
            assert isinstance(name, str)
            assert isinstance(value, (int, float))
            if tags is not None:
                self.__validate_tags(tags)

    def timing(
        self, name: str, value: Union[int, float], tags: Optional[Tags] = None
    ) -> None:
        if self.__strict:
            assert isinstance(name, str)
            assert isinstance(value, (int, float))
            if tags is not None:
                self.__validate_tags(tags)
