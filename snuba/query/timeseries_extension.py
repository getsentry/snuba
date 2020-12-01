from datetime import datetime, timedelta
from typing import Any, Mapping, Tuple

from snuba import environment, state
from snuba.query.conditions import (
    BooleanFunctions,
    ConditionFunctions,
    binary_condition,
)
from snuba.query.expressions import Column, Literal
from snuba.query.extensions import QueryExtension
from snuba.query.logical import Query
from snuba.query.processors import ExtensionData, ExtensionQueryProcessor
from snuba.request.request_settings import RequestSettings
from snuba.util import parse_datetime
from snuba.utils.metrics.decorators import track_calls
from snuba.utils.metrics.wrapper import MetricsWrapper


timeseries_metrics = MetricsWrapper(environment.metrics, "extensions.timeseries")


def get_time_series_extension_properties(
    default_granularity: int, default_window: timedelta
):
    return {
        "type": "object",
        "properties": {
            "from_date": {
                "type": "string",
                "format": "date-time",
                "default": track_calls(
                    timeseries_metrics,
                    "from_date.default",
                    lambda: (
                        datetime.utcnow().replace(microsecond=0) - default_window
                    ).isoformat(),
                ),
            },
            "to_date": {
                "type": "string",
                "format": "date-time",
                "default": track_calls(
                    timeseries_metrics,
                    "to_date.default",
                    lambda: datetime.utcnow().replace(microsecond=0).isoformat(),
                ),
            },
            "granularity": {
                "type": "number",
                "default": default_granularity,
                "minimum": 1,
            },
        },
        "additionalProperties": False,
    }


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
        query.add_condition_to_ast(
            binary_condition(
                BooleanFunctions.AND,
                binary_condition(
                    ConditionFunctions.GTE,
                    Column(None, None, self.__timestamp_column),
                    Literal(None, from_date),
                ),
                binary_condition(
                    ConditionFunctions.LT,
                    Column(None, None, self.__timestamp_column),
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
