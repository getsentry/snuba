from datetime import datetime, timedelta
from typing import Any, Mapping, Tuple

from snuba import state
from snuba.util import parse_datetime
from snuba.query.conditions import (
    binary_condition,
    BooleanFunctions,
    ConditionFunctions,
)
from snuba.query.expressions import Column, Literal
from snuba.query.extensions import QueryExtension
from snuba.query.logical import Query
from snuba.query.processors import ExtensionData, ExtensionQueryProcessor
from snuba.request.request_settings import RequestSettings
from snuba.schemas import get_time_series_extension_properties


class TimeSeriesExtensionProcessor(ExtensionQueryProcessor):
    def __init__(self, timstamp_column: str):
        self.__timestamp_column = timstamp_column

    @classmethod
    def get_time_limit(
        cls, timeseries_extension: Mapping[str, Any]
    ) -> Tuple[datetime, datetime]:
        max_days, date_align = state.get_configs(
            [("max_days", None), ("date_align_seconds", 1)]
        )

        to_date = parse_datetime(timeseries_extension["to_date"], date_align)
        from_date = parse_datetime(timeseries_extension["from_date"], date_align)
        assert from_date <= to_date

        if max_days is not None and (to_date - from_date).days > max_days:
            from_date = to_date - timedelta(days=max_days)

        return (from_date, to_date)

    def process_query(
        self,
        query: Query,
        extension_data: ExtensionData,
        request_settings: RequestSettings,
    ) -> None:
        from_date, to_date = self.get_time_limit(extension_data)
        query.set_granularity(extension_data["granularity"])
        query.add_conditions(
            [
                (self.__timestamp_column, ">=", from_date.isoformat()),
                (self.__timestamp_column, "<", to_date.isoformat()),
            ]
        )
        query.add_condition_to_ast(
            binary_condition(
                None,
                BooleanFunctions.AND,
                binary_condition(
                    None,
                    ConditionFunctions.GTE,
                    Column(None, self.__timestamp_column, None),
                    Literal(None, from_date),
                ),
                binary_condition(
                    None,
                    ConditionFunctions.LT,
                    Column(None, self.__timestamp_column, None),
                    Literal(None, to_date),
                ),
            )
        )


class TimeSeriesExtension(QueryExtension):
    def __init__(
        self,
        default_granularity: int,
        default_window: timedelta,
        timestamp_column: str,
    ) -> None:
        super().__init__(
            schema=get_time_series_extension_properties(
                default_granularity=default_granularity, default_window=default_window,
            ),
            processor=TimeSeriesExtensionProcessor(timestamp_column),
        )
