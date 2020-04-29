import pytest
import uuid

from typing import Any, MutableMapping, Sequence

from snuba import state
from snuba.clickhouse.columns import ColumnSet
from snuba.datasets.factory import get_dataset
from snuba.datasets.plans.single_storage import SimpleQueryPlanExecutionStrategy
from snuba.web.split import (
    ColumnSplitQueryStrategy,
    TimeSplitQueryStrategy,
)
from snuba.datasets.schemas.tables import TableSource
from snuba.query.query import Query
from snuba.request import Request
from snuba.request.request_settings import HTTPRequestSettings
from snuba.web import QueryResult


def setup_function(function) -> None:
    state.set_config("use_split", 1)


split_specs = [
    ("events", "event_id", "project_id", "timestamp",),
    ("groups", "events.event_id", "events.project_id", "events.timestamp",),
]


@pytest.mark.parametrize(
    "dataset_name, id_column, project_column, timestamp_column", split_specs
)
def test_no_split(
    dataset_name: str, id_column: str, project_column: str, timestamp_column: str
) -> None:
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

    def do_query(request: Request) -> QueryResult:
        assert request.query == query
        return QueryResult({}, {})

    request = Request(uuid.uuid4().hex, query, HTTPRequestSettings(), {}, "tests")

    strategy = SimpleQueryPlanExecutionStrategy(
        [
            ColumnSplitQueryStrategy(
                id_column=id_column,
                project_column=project_column,
                timestamp_column=timestamp_column,
            ),
            TimeSplitQueryStrategy(timestamp_col=timestamp_column),
        ]
    )

    strategy.execute(request, do_query)


test_data_col = [
    (
        "events",
        "event_id",
        "project_id",
        "timestamp",
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
        "events.event_id",
        "events.project_id",
        "events.timestamp",
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
    "dataset_name, id_column, project_column, timestamp_column, first_query_data, second_query_data",
    test_data_col,
)
def test_col_split(
    dataset_name: str,
    id_column: str,
    project_column: str,
    timestamp_column: str,
    first_query_data: Sequence[MutableMapping[str, Any]],
    second_query_data: Sequence[MutableMapping[str, Any]],
) -> None:
    def do_query(request: Request) -> QueryResult:
        selected_cols = request.query.get_selected_columns()
        if selected_cols == list(first_query_data[0].keys()):
            return QueryResult({"data": first_query_data}, {})
        elif selected_cols == list(second_query_data[0].keys()):
            return QueryResult({"data": second_query_data}, {})
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

    strategy = SimpleQueryPlanExecutionStrategy(
        [
            ColumnSplitQueryStrategy(id_column, project_column, timestamp_column),
            TimeSplitQueryStrategy(timestamp_col=timestamp_column),
        ]
    )

    strategy.execute(request, do_query)


column_split_tests = [
    (
        "event_id",
        "project_id",
        "timestamp",
        Query(
            {
                "selected_columns": [
                    "event_id",
                    "level",
                    "logger",
                    "server_name",
                    "transaction",
                    "timestamp",
                    "project_id",
                ],
                "conditions": [
                    ("timestamp", ">=", "2019-09-19T10:00:00"),
                    ("timestamp", "<", "2019-09-19T12:00:00"),
                    ("project_id", "IN", [1, 2, 3]),
                ],
                "groupby": ["timestamp"],
                "limit": 10,
            },
            TableSource("events", ColumnSet([])),
        ),
        False,
    ),  # Query with group by. No split
    (
        "event_id",
        "project_id",
        "timestamp",
        Query(
            {
                "selected_columns": [
                    "event_id",
                    "level",
                    "logger",
                    "server_name",
                    "transaction",
                    "timestamp",
                    "project_id",
                ],
                "conditions": [
                    ("timestamp", ">=", "2019-09-19T10:00:00"),
                    ("timestamp", "<", "2019-09-19T12:00:00"),
                    ("project_id", "IN", [1, 2, 3]),
                ],
                "limit": 10,
            },
            TableSource("events", ColumnSet([])),
        ),
        True,
    ),  # Valid query to split
    (
        "event_id",
        "project_id",
        "timestamp",
        Query(
            {
                "selected_columns": ["event_id"],
                "conditions": [
                    ("timestamp", ">=", "2019-09-19T10:00:00"),
                    ("timestamp", "<", "2019-09-19T12:00:00"),
                    ("project_id", "IN", [1, 2, 3]),
                ],
                "limit": 10,
            },
            TableSource("events", ColumnSet([])),
        ),
        False,
    ),  # Valid query but not enough columns to split.
]


@pytest.mark.parametrize(
    "id_column, project_column, timestamp_column, query, expected_result",
    column_split_tests,
)
def test_col_split_conditions(
    id_column: str, project_column: str, timestamp_column: str, query, expected_result
) -> None:
    splitter = ColumnSplitQueryStrategy(id_column, project_column, timestamp_column)
    request = Request(uuid.uuid4().hex, query, HTTPRequestSettings(), {}, "tests")

    def do_query(request: Request) -> QueryResult:
        return QueryResult({"data": []}, {})

    assert (splitter.execute(request, do_query) is not None) == expected_result
