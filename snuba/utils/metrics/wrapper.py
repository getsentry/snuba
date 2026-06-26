from snuba.utils.metrics.backends.abstract import MetricsBackend
from snuba.utils.metrics.types import Tags


class MetricsWrapper(MetricsBackend):
    def __init__(
        self,
        backend: MetricsBackend,
        name: str | None = None,
        tags: Tags | None = None,
    ) -> None:
        self.__backend = backend
        self.__name = name
        self.__tags = tags

    def __merge_name(self, name: str) -> str:
        if self.__name is None:
            return name
        return f"{self.__name}.{name}"

    def __merge_tags(self, tags: Tags | None) -> Tags | None:
        if self.__tags is None:
            return tags
        if tags is None:
            return self.__tags
        return {**tags, **self.__tags}

    def increment(
        self,
        name: str,
        value: int | float = 1,
        tags: Tags | None = None,
        unit: str | None = None,
    ) -> None:
        self.__backend.increment(self.__merge_name(name), value, self.__merge_tags(tags), unit)

    def gauge(
        self,
        name: str,
        value: int | float,
        tags: Tags | None = None,
        unit: str | None = None,
    ) -> None:
        self.__backend.gauge(self.__merge_name(name), value, self.__merge_tags(tags), unit)

    def timing(
        self,
        name: str,
        value: int | float,
        tags: Tags | None = None,
        unit: str | None = None,
    ) -> None:
        self.__backend.timing(self.__merge_name(name), value, self.__merge_tags(tags), unit)

    def distribution(
        self,
        name: str,
        value: int | float,
        tags: Tags | None = None,
        unit: str | None = None,
    ) -> None:
        self.__backend.distribution(self.__merge_name(name), value, self.__merge_tags(tags), unit)

    def events(
        self,
        title: str,
        text: str,
        alert_type: str,
        priority: str,
        tags: Tags | None = None,
    ) -> None:
        self.__backend.events(title, text, alert_type, priority, self.__merge_tags(tags))
