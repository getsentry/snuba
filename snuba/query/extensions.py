from abc import ABC, abstractmethod
from datetime import timedelta

from snuba.query.query_processor import (
    DummyExtensionProcessor,
    ExtensionQueryProcessor
)
from snuba.schemas import (
    get_time_series_extension_properties,
    PERFORMANCE_EXTENSION_SCHEMA,
    PROJECT_EXTENSION_SCHEMA,
    Schema
)


class QueryExtension(ABC):
    """
    Defines a query extension by coupling a query extension schema
    and a query extension processor that updates the query by
    processing the extension data.
    """

    @abstractmethod
    def get_schema(self) -> Schema:
        raise NotImplementedError

    @abstractmethod
    def get_processor(self) -> ExtensionQueryProcessor:
        raise NotImplementedError


class PerformanceExtension(QueryExtension):
    def get_schema(self) -> Schema:
        return PERFORMANCE_EXTENSION_SCHEMA

    def get_processor(self) -> ExtensionQueryProcessor:
        return DummyExtensionProcessor()


class ProjectExtension(QueryExtension):
    def get_schema(self) -> Schema:
        return PROJECT_EXTENSION_SCHEMA

    def get_processor(self) -> ExtensionQueryProcessor:
        return DummyExtensionProcessor()


class TimeseriesExtension(QueryExtension):
    def __init__(
        self,
        default_granularity: int,
        default_window: timedelta,
    ) -> None:
        self.__default_granularity = default_granularity
        self.__default_window = default_window

    def get_schema(self) -> Schema:
        return get_time_series_extension_properties(
            default_granularity=self.__default_granularity,
            default_window=self.__default_window,
        )

    def get_processor(self) -> ExtensionQueryProcessor:
        return DummyExtensionProcessor()
