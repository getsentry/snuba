from typing import Any, MutableMapping, Sequence

import pytest
import uuid

from snuba import state
from snuba.datasets.factory import get_dataset
from snuba.datasets.plans.single_table import SimpleQueryPlanExecutionStrategy
from snuba.datasets.plans.split import SplitQueryPlanExecutionStrategy, ColumnSplitSpec
from snuba.query import RawQueryResult
from snuba.query.query import Query
from snuba.request import Request
from snuba.request.request_settings import HTTPRequestSettings


def setup_function(function) -> None:
    state.set_config("use_split", 1)


split_specs = [
    (
        "events",
        ColumnSplitSpec(
            id_column="event_id",
            project_column="project_id",
            timestamp_column="timestamp",
        ),
    ),
    (
        "groups",
        ColumnSplitSpec(
            id_column="events.event_id",
            project_column="events.project_id",
            timestamp_column="events.timestamp",
        ),
    ),
]


@pytest.mark.parametrize("dataset_name, split_spec", split_specs)
def test_no_split(dataset_name: str, split_spec: ColumnSplitSpec) -> None:
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
        events.get_all_storages()[0].get_schemas().get_read_schema().get_data_source(),
    )

    def do_query(request: Request) -> RawQueryResult:
        assert request.query == query
        return RawQueryResult({}, {})

    request = Request(uuid.uuid4().hex, query, HTTPRequestSettings(), {}, "tests")

    strategy = SplitQueryPlanExecutionStrategy(
        split_spec=split_spec, default_strategy=SimpleQueryPlanExecutionStrategy(),
    )

    strategy.execute(request, do_query)


test_data_col = [
    (
        "events",
        ColumnSplitSpec(
            id_column="event_id",
            project_column="project_id",
            timestamp_column="timestamp",
        ),
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
        ColumnSplitSpec(
            id_column="events.event_id",
            project_column="events.project_id",
            timestamp_column="events.timestamp",
        ),
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
    "dataset_name, split_spec, first_query_data, second_query_data", test_data_col
)
def test_col_split(
    dataset_name: str,
    split_spec: ColumnSplitSpec,
    first_query_data: Sequence[MutableMapping[str, Any]],
    second_query_data: Sequence[MutableMapping[str, Any]],
) -> None:
    def do_query(request: Request) -> RawQueryResult:
        selected_cols = request.query.get_selected_columns()
        if selected_cols == list(first_query_data[0].keys()):
            return RawQueryResult({"data": first_query_data}, None, {})
        elif selected_cols == list(second_query_data[0].keys()):
            return RawQueryResult({"data": second_query_data}, None, {})
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
        events.get_all_storages()[0].get_schemas().get_read_schema().get_data_source(),
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

    strategy = SplitQueryPlanExecutionStrategy(
        split_spec=split_spec, default_strategy=SimpleQueryPlanExecutionStrategy(),
    )

    strategy.execute(request, do_query)
