from typing import Optional, Union

from snuba.utils.metrics.backends.abstract import MetricsBackend
from snuba.utils.metrics.types import Tags


class MetricsWrapper(MetricsBackend):
    def __init__(
        self,
        backend: MetricsBackend,
        name: Optional[str] = None,
        tags: Optional[Tags] = None,
    ) -> None:
        self.__backend = backend
        self.__name = name
        self.__tags = tags

    def __merge_name(self, name: str) -> str:
        if self.__name is None:
            return name
        else:
            return f"{self.__name}.{name}"

    def __merge_tags(self, tags: Optional[Tags]) -> Optional[Tags]:
        if self.__tags is None:
            return tags
        elif tags is None:
            return self.__tags
        else:
            return {**tags, **self.__tags}

    def increment(
        self, name: str, value: Union[int, float] = 1, tags: Optional[Tags] = None
    ) -> None:
        self.__backend.increment(
            self.__merge_name(name), value, self.__merge_tags(tags)
        )

    def gauge(
        self, name: str, value: Union[int, float], tags: Optional[Tags] = None
    ) -> None:
        self.__backend.gauge(self.__merge_name(name), value, self.__merge_tags(tags))

    def timing(
        self, name: str, value: Union[int, float], tags: Optional[Tags] = None
    ) -> None:
        self.__backend.timing(self.__merge_name(name), value, self.__merge_tags(tags))

    def events(
        self,
        title: str,
        text: str,
        alert_type: str,
        priority: str,
        tags: Optional[Tags] = None,
    ) -> None:
        self.__backend.events(
            title, text, alert_type, priority, self.__merge_tags(tags)
        )
