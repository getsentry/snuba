import pytest
import datetime
from typing import Sequence

from snuba import state
from snuba.query.timeseries import TimeSeriesExtension
from snuba.query.query import Query, Condition
from snuba.schemas import validate_jsonschema


test_data = [
    (
        {
            "from_date": "2019-09-19T10:00:00",
            "to_date": "2019-09-19T12:00:00",
            "granularity": 3600,
        },
        [
            ("timestamp", ">=", "2019-09-19T10:00:00"),
            ("timestamp", "<", "2019-09-19T12:00:00"),
        ]
    ),
    (
        {
            "from_date": "1970-01-01T10:00:00",
            "to_date": "2019-09-19T12:00:00",
            "granularity": 3600,
        },
        [
            ("timestamp", ">=", "2019-09-18T12:00:00"),
            ("timestamp", "<", "2019-09-19T12:00:00"),
        ]
    ),
    (
        {
            "from_date": "2019-09-19T10:05:30,1234",
            "to_date": "2019-09-19T12:00:34,4567",
            "granularity": 3600,
        },
        [
            ("timestamp", ">=", "2019-09-19T10:05:30"),
            ("timestamp", "<", "2019-09-19T12:00:34"),
        ]
    )
]


@pytest.mark.parametrize("raw_data, expected_conditions", test_data)
def test_query_extension_processing(raw_data: dict, expected_conditions: Sequence[Condition]):
    state.set_config('max_days', 1)
    extension = TimeSeriesExtension(
        default_granularity=3600,
        default_window=datetime.timedelta(days=5),
        timestamp_column='timestamp',
    )
    valid_data = validate_jsonschema(raw_data, extension.get_schema())
    query = Query({
        "conditions": []
    })

    extension.get_processor().process_query(query, valid_data, {}, {})
    assert query.get_conditions() == expected_conditions
