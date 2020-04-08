import pytest
import uuid

from typing import Any, Mapping

from snuba import state
from snuba.datasets.dataset import Dataset
from snuba.datasets.factory import get_dataset
from snuba.query.query import Query
from snuba.request import Request
from snuba.request.request_settings import HTTPRequestSettings
from snuba.utils.metrics.timer import Timer
from snuba.web import RawQueryResult
from snuba.web.split import split_query


def setup_function(function):
    state.set_config("use_split", 1)


test_data_no_split = [
    "events",
    "transactions",
    "groups",
]


@pytest.mark.parametrize("dataset_name", test_data_no_split)
def test_no_split(dataset_name: str):
    events = get_dataset(dataset_name)
    query = Query(
        {
            "selected_columns": ["event_id"],
            "conditions": [""],
            "orderby": "event_id",
            "sample": 10,
            "limit": 100,
            "offset": 50,
        },
        events.get_all_storages()[0]
        .get_schemas()
        .get_read_schema()
        .get_data_source(),
    )

    @split_query
    def do_query(dataset: Dataset, request: Request, timer: Timer):
        assert request.query == query

    request = Request(uuid.uuid4().hex, query, HTTPRequestSettings(), {}, "tests")

    do_query(events, request, None)


test_data_col = [
    (
        "events",
        [{"event_id": "a", "project_id": "1", "timestamp": " 2019-10-01 22:33:42"}],
        [
            {
                "event_id": "a",
                "project_id": "1",
                "level": "error",
                "timestamp": " 2019-10-01 22:33:42",
            }
        ],
    ),
    (
        "groups",
        [
            {
                "events.event_id": "a",
                "events.project_id": "1",
                "events.timestamp": " 2019-10-01 22:33:42",
            }
        ],
        [
            {
                "events.event_id": "a",
                "events.project_id": "1",
                "events.level": "error",
                "events.timestamp": " 2019-10-01 22:33:42",
            }
        ],
    ),
]


@pytest.mark.parametrize(
    "dataset_name, first_query_data, second_query_data", test_data_col
)
def test_col_split(
    dataset_name: str,
    first_query_data: Mapping[str, Any],
    second_query_data: Mapping[str, Any],
):
    @split_query
    def do_query(dataset: Dataset, request: Request, timer: Timer):
        selected_cols = request.query.get_selected_columns()
        if selected_cols == list(first_query_data[0].keys()):
            return RawQueryResult({"data": first_query_data}, {})
        elif selected_cols == list(second_query_data[0].keys()):
            return RawQueryResult({"data": second_query_data}, {})
        else:
            raise ValueError(f"Unexpected selected columns: {selected_cols}")

    events = get_dataset(dataset_name)
    query = Query(
        {
            "selected_columns": list(second_query_data[0].keys()),
            "conditions": [""],
            "orderby": "events.event_id",
            "sample": 10,
            "limit": 100,
            "offset": 50,
        },
        events.get_all_storages()[0]
        .get_schemas()
        .get_read_schema()
        .get_data_source(),
    )

    request = Request(
        uuid.uuid4().hex,
        query,
        HTTPRequestSettings(),
        {
            "project": {"project": 1},
            "timeseries": {
                "from_date": "2019-09-19T10:00:00",
                "to_date": "2019-09-19T12:00:00",
                "granularity": 3600,
            },
        },
        "tests",
    )

    do_query(events, request, None)
