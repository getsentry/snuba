from abc import ABC, abstractstaticmethod
from datetime import datetime, timedelta
from mypy_extensions import TypedDict
from typing import Any, Generic, Mapping, Tuple, TypeVar

from snuba import state
from snuba import util
from snuba.query.query import Query
from snuba.query.query_processor import (
    DummyExtensionProcessor,
    QueryProcessor,
)
from snuba.schemas import (
    get_time_series_extension_properties,
    Schema
)

TExtensionPayload = TypeVar("TExtensionPayload")


class QueryExtension(ABC, Generic[TExtensionPayload]):
    """
    Defines a query extension by coupling a query extension schema
    and a query extension processor that updates the query by
    processing the extension data.
    """

    def __init__(self,
        schema: Schema,
        processor: QueryProcessor[TExtensionPayload],
    ) -> None:
        self.__schema = schema
        self.__processor = processor

    def get_schema(self) -> Schema:
        return self.__schema

    @abstractstaticmethod
    def parse_payload(cls, payload: Mapping[str, Any]) -> TExtensionPayload:
        raise NotImplementedError

    def get_processor(self) -> QueryProcessor[TExtensionPayload]:
        return self.__processor


PROJECT_EXTENSION_SCHEMA = {
    'type': 'object',
    'properties': {
        'project': {
            'anyOf': [
                {'type': 'integer', 'minimum': 1},
                {
                    'type': 'array',
                    'items': {'type': 'integer', 'minimum': 1},
                    'minItems': 1,
                },
            ]
        },
    },
    # Need to select down to the project level for customer isolation and performance
    'required': ['project'],
    'additionalProperties': False,
}

PERFORMANCE_EXTENSION_SCHEMA = {
    'type': 'object',
    'properties': {
        # Never add FINAL to queries, enable sampling
        'turbo': {
            'type': 'boolean',
            'default': False,
        },
        # Force queries to hit the first shard replica, ensuring the query
        # sees data that was written before the query. This burdens the
        # first replica, so should only be used when absolutely necessary.
        'consistent': {
            'type': 'boolean',
            'default': False,
        },
        'debug': {
            'type': 'boolean',
            'default': False,
        },
    },
    'additionalProperties': False,
}


class PerformanceExtension(QueryExtension[Any]):
    def __init__(self) -> None:
        super().__init__(
            schema=PERFORMANCE_EXTENSION_SCHEMA,
            processor=DummyExtensionProcessor(),
        )

    def parse_payload(cls, payload: Mapping[str, Any]) -> Any:
        return payload


class ProjectExtension(QueryExtension[Any]):
    def __init__(self) -> None:
        super().__init__(
            schema=PROJECT_EXTENSION_SCHEMA,
            processor=DummyExtensionProcessor(),
        )

    def parse_payload(cls, payload: Mapping[str, Any]) -> Any:
        return payload


class TimeSeriesExtensionPayload(TypedDict):
    from_date: str
    to_date: str
    granularity: int


class TimeSeriesExtensionProcessor(QueryProcessor[TimeSeriesExtensionPayload]):
    def __init__(self, timstamp_column: str):
        self.__timestamp_column = timstamp_column

    @classmethod
    def get_time_limit(cls, timeseries_extension: TimeSeriesExtensionPayload) -> Tuple[datetime, datetime]:
        max_days, date_align = state.get_configs([
            ('max_days', None),
            ('date_align_seconds', 1),
        ])

        to_date = util.parse_datetime(timeseries_extension['to_date'], date_align)
        from_date = util.parse_datetime(timeseries_extension['from_date'], date_align)
        assert from_date <= to_date

        if max_days is not None and (to_date - from_date).days > max_days:
            from_date = to_date - timedelta(days=max_days)

        return (from_date, to_date)

    def process_query(self, query: Query, extension_data: TimeSeriesExtensionPayload) -> None:
        from_date, to_date = self.get_time_limit(extension_data)
        query.add_conditions([
            (self.__timestamp_column, '>=', from_date.isoformat()),
            (self.__timestamp_column, '<', to_date.isoformat()),
        ])


class TimeSeriesExtension(QueryExtension[TimeSeriesExtensionPayload]):
    def __init__(
        self,
        default_granularity: int,
        default_window: timedelta,
        timestamp_column: str,
    ) -> None:
        super().__init__(
            schema=get_time_series_extension_properties(
                default_granularity=default_granularity,
                default_window=default_window,
            ),
            processor=TimeSeriesExtensionProcessor(timestamp_column),
        )

    @classmethod
    def parse_payload(cls, payload: Mapping[str, Any]) -> TimeSeriesExtensionPayload:
        return TimeSeriesExtensionPayload(
            from_date=payload["from_date"],
            to_date=payload["to_date"],
            granularity=payload["granularity"],
        )
