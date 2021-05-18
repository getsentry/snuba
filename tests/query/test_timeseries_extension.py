from datetime import datetime, timedelta
from typing import Any, MutableMapping, cast

import pytest

from snuba import state
from snuba.clickhouse.columns import ColumnSet
from snuba.datasets.entities import EntityKey
from snuba.query.conditions import (
    BooleanFunctions,
    ConditionFunctions,
    binary_condition,
)
from snuba.query.data_source.simple import Entity as QueryEntity
from snuba.query.expressions import Column, Expression, Literal
from snuba.query.logical import Query
from snuba.query.timeseries_extension import TimeSeriesExtension
from snuba.request.request_settings import HTTPRequestSettings
from snuba.schemas import validate_jsonschema


def build_time_condition(
    time_columns: str, from_date: datetime, to_date: datetime
) -> Expression:
    return binary_condition(
        BooleanFunctions.AND,
        binary_condition(
            ConditionFunctions.GTE,
            Column(f"_snuba_{time_columns}", None, time_columns),
            Literal(None, from_date),
        ),
        binary_condition(
            ConditionFunctions.LT,
            Column(f"_snuba_{time_columns}", None, time_columns),
            Literal(None, to_date),
        ),
    )


test_data = [
    (
        {
            "from_date": "2019-09-19T10:00:00",
            "to_date": "2019-09-19T12:00:00",
            "granularity": 3600,
        },
        build_time_condition(
            "timestamp", datetime(2019, 9, 19, 10), datetime(2019, 9, 19, 12)
        ),
        3600,
    ),
    (
        {
            "from_date": "1970-01-01T10:00:00",
            "to_date": "2019-09-19T12:00:00",
            "granularity": 3600,
        },
        build_time_condition(
            "timestamp", datetime(2019, 9, 18, 12), datetime(2019, 9, 19, 12)
        ),
        3600,
    ),
    (
        {
            "from_date": "2019-09-19T10:05:30,1234",
            "to_date": "2019-09-19T12:00:34,4567",
        },
        build_time_condition(
            "timestamp",
            datetime(2019, 9, 19, 10, 5, 30),
            datetime(2019, 9, 19, 12, 0, 34),
        ),
        60,
    ),
]


@pytest.mark.parametrize(
    "raw_data, expected_ast_condition, expected_granularity", test_data,
)
def test_query_extension_processing(
    raw_data: MutableMapping[str, Any],
    expected_ast_condition: Expression,
    expected_granularity: int,
) -> None:
    state.set_config("max_days", 1)
    extension = TimeSeriesExtension(
        default_granularity=60,
        default_window=timedelta(days=5),
        timestamp_column="timestamp",
    )

    valid_data = validate_jsonschema(
        raw_data, cast(MutableMapping[str, Any], extension.get_schema())
    )
    query = Query(QueryEntity(EntityKey.EVENTS, ColumnSet([])))

    request_settings = HTTPRequestSettings()

    extension.get_processor().process_query(query, valid_data, request_settings)
    assert query.get_condition() == expected_ast_condition
    assert query.get_granularity() == expected_granularity
