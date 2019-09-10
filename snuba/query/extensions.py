from abc import ABC
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

    def __init__(self,
        schema: Schema,
        processor: ExtensionQueryProcessor,
    ) -> None:
        self.__schema = schema
        self.__processor = processor

    def get_schema(self) -> Schema:
        return self.__schema

    def get_processor(self) -> ExtensionQueryProcessor:
        return self.__processor


class PerformanceExtension(QueryExtension):
    def __init__(self) -> None:
        super().__init__(
            schema=PERFORMANCE_EXTENSION_SCHEMA,
            processor=DummyExtensionProcessor(),
        )


class ProjectExtension(QueryExtension):
    def __init__(self) -> None:
        super().__init__(
            schema=PROJECT_EXTENSION_SCHEMA,
            processor=DummyExtensionProcessor(),
        )


class TimeseriesExtension(QueryExtension):
    def __init__(
        self,
        default_granularity: int,
        default_window: timedelta,
    ) -> None:
        super().__init__(
            schema=get_time_series_extension_properties(
                default_granularity=default_granularity,
                default_window=default_window,
            ),
            processor=DummyExtensionProcessor(),
        )
