from datetime import datetime, timedelta
from mypy_extensions import TypedDict
from typing import Any, Mapping, Tuple

from snuba import state
from snuba.util import parse_datetime
from snuba.query.extensions import QueryExtension
from snuba.query.query_processor import QueryProcessor
from snuba.query.query import Query
from snuba.schemas import get_time_series_extension_properties


class TimeSeriesExtensionPayload(TypedDict):
    from_date: str
    to_date: str
    granularity: int


class TimeSeriesExtensionProcessor(QueryProcessor[TimeSeriesExtensionPayload]):
    def __init__(self, timstamp_column: str):
        self.__timestamp_column = timstamp_column

    @classmethod
    def get_time_limit(cls, timeseries_extension: Mapping[str, Any]) -> Tuple[datetime, datetime]:
        max_days, date_align = state.get_configs([
            ('max_days', None),
            ('date_align_seconds', 1),
        ])

        to_date = parse_datetime(timeseries_extension['to_date'], date_align)
        from_date = parse_datetime(timeseries_extension['from_date'], date_align)
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
