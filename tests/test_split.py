from datetime import datetime
from typing import Any, MutableMapping, Sequence

import pytest

from snuba import state
from snuba.clickhouse.columns import ColumnSet, String
from snuba.clickhouse.query import Query as ClickhouseQuery
from snuba.clickhouse.query_dsl.accessors import get_time_range
from snuba.clickhouse.sql import SqlQuery
from snuba.clusters.cluster import ClickhouseCluster
from snuba.datasets.entities.factory import EntityKey, get_entity
from snuba.datasets.factory import get_dataset
from snuba.datasets.plans.single_storage import SimpleQueryPlanExecutionStrategy
from snuba.query.expressions import Column
from snuba.query.logical import Query as LogicalQuery
from snuba.query.logical import SelectedExpression
from snuba.query.parser import parse_query
from snuba.reader import Reader
from snuba.request import Request
from snuba.request.request_settings import HTTPRequestSettings, RequestSettings
from snuba.web import QueryResult
from snuba.web.split import ColumnSplitQueryStrategy, TimeSplitQueryStrategy


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
    query = ClickhouseQuery(
        LogicalQuery(
            {
                "selected_columns": ["event_id"],
                "conditions": [""],
                "orderby": "event_id",
                "sample": 10,
                "limit": 100,
                "offset": 50,
            },
            events.get_all_storages()[0].get_schema().get_data_source(),
        )
    )

    def do_query(
        query: ClickhouseQuery,
        request_settings: RequestSettings,
        reader: Reader[SqlQuery],
    ) -> QueryResult:
        assert query == query
        return QueryResult({}, {})

    strategy = SimpleQueryPlanExecutionStrategy(
        ClickhouseCluster("localhost", 1024, "default", "", "default", 80, set(), True),
        [],
        [
            ColumnSplitQueryStrategy(
                id_column=id_column,
                project_column=project_column,
                timestamp_column=timestamp_column,
            ),
            TimeSplitQueryStrategy(timestamp_col=timestamp_column),
        ],
    )

    strategy.execute(query, HTTPRequestSettings(), do_query)


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
                "events.timestamp": "2019-10-01 22:33:42",
            }
        ],
        [
            {
                "events.event_id": "a",
                "events.project_id": "1",
                "events.level": "error",
                "events.timestamp": "2019-10-01 22:33:42",
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
    def do_query(
        query: ClickhouseQuery,
        request_settings: RequestSettings,
        reader: Reader[SqlQuery],
    ) -> QueryResult:
        selected_col_names = [
            c.expression.column_name
            for c in query.get_selected_columns_from_ast() or []
            if isinstance(c.expression, Column)
        ]
        if selected_col_names == list(first_query_data[0].keys()):
            return QueryResult({"data": first_query_data}, {})
        elif selected_col_names == list(second_query_data[0].keys()):
            return QueryResult({"data": second_query_data}, {})
        else:
            raise ValueError(f"Unexpected selected columns: {selected_col_names}")

    events = get_dataset(dataset_name)
    query = ClickhouseQuery(
        LogicalQuery(
            {
                "selected_columns": list(second_query_data[0].keys()),
                "conditions": [""],
                "orderby": "events.event_id",
                "sample": 10,
                "limit": 100,
                "offset": 50,
            },
            events.get_all_storages()[0].get_schema().get_data_source(),
            selected_columns=[
                SelectedExpression(
                    name=col_name, expression=Column(None, None, col_name)
                )
                for col_name in second_query_data[0].keys()
            ],
        )
    )

    strategy = SimpleQueryPlanExecutionStrategy(
        ClickhouseCluster("localhost", 1024, "default", "", "default", 80, set(), True),
        [],
        [
            ColumnSplitQueryStrategy(id_column, project_column, timestamp_column),
            TimeSplitQueryStrategy(timestamp_col=timestamp_column),
        ],
    )

    strategy.execute(query, HTTPRequestSettings(), do_query)


column_set = ColumnSet(
    [
        ("event_id", String()),
        ("project_id", String()),
        ("timestamp", String()),
        ("level", String()),
        ("logger", String()),
        ("server_name", String()),
        ("transaction", String()),
    ]
)

column_split_tests = [
    (
        "event_id",
        "project_id",
        "timestamp",
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
        False,
    ),  # Query with group by. No split
    (
        "event_id",
        "project_id",
        "timestamp",
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
        True,
    ),  # Valid query to split
    (
        "event_id",
        "project_id",
        "timestamp",
        {
            "selected_columns": ["event_id"],
            "conditions": [
                ("timestamp", ">=", "2019-09-19T10:00:00"),
                ("timestamp", "<", "2019-09-19T12:00:00"),
                ("project_id", "IN", [1, 2, 3]),
            ],
            "limit": 10,
        },
        False,
    ),  # Valid query but not enough columns to split.
    (
        "event_id",
        "project_id",
        "timestamp",
        {
            "selected_columns": ["group_id"],
            "conditions": [
                ("timestamp", ">=", "2019-09-19T10:00:00"),
                ("timestamp", "<", "2019-09-19T12:00:00"),
                ("project_id", "IN", [1, 2, 3]),
                ("event_id", "=", "a" * 32),
            ],
            "limit": 10,
        },
        False,
    ),  # Query with = on event_id, not split
    (
        "event_id",
        "project_id",
        "timestamp",
        {
            "selected_columns": ["group_id"],
            "conditions": [
                ("timestamp", ">=", "2019-09-19T10:00:00"),
                ("timestamp", "<", "2019-09-19T12:00:00"),
                ("project_id", "IN", [1, 2, 3]),
                ("event_id", "IN", ["a" * 32, "b" * 32]),
            ],
            "limit": 10,
        },
        False,
    ),  # Query with IN on event_id - do not split
    (
        "event_id",
        "project_id",
        "timestamp",
        {
            "selected_columns": ["group_id"],
            "conditions": [
                ("timestamp", ">=", "2019-09-19T10:00:00"),
                ("timestamp", "<", "2019-09-19T12:00:00"),
                ("project_id", "IN", [1, 2, 3]),
                ("event_id", ">", "a" * 32),
            ],
            "limit": 10,
        },
        True,
    ),  # Query with other condition on event_id - proceed with split
]


@pytest.mark.parametrize(
    "id_column, project_column, timestamp_column, query, expected_result",
    column_split_tests,
)
def test_col_split_conditions(
    id_column: str, project_column: str, timestamp_column: str, query, expected_result
) -> None:
    dataset = get_dataset("events")
    query = parse_query(query, dataset)
    splitter = ColumnSplitQueryStrategy(id_column, project_column, timestamp_column)
    request = Request("a", query, HTTPRequestSettings(), {}, "r")
    entity = get_entity(EntityKey(query.get_entity_name()))
    plan = entity.get_query_plan_builder().build_plan(request)

    def do_query(
        query: ClickhouseQuery, request_settings: RequestSettings,
    ) -> QueryResult:
        return QueryResult(
            {
                "data": [
                    {
                        id_column: "asd123",
                        project_column: 123,
                        timestamp_column: "2019-10-01 22:33:42",
                    }
                ]
            },
            {},
        )

    assert (
        splitter.execute(plan.query, HTTPRequestSettings(), do_query) is not None
    ) == expected_result


def test_time_split_ast() -> None:
    """
    Test that the time split transforms the query properly both on the old representation
    and on the AST representation.
    """
    found_timestamps = []

    def do_query(
        query: ClickhouseQuery, request_settings: RequestSettings,
    ) -> QueryResult:
        from_date_ast, to_date_ast = get_time_range(query, "timestamp")
        assert from_date_ast is not None and isinstance(from_date_ast, datetime)
        assert to_date_ast is not None and isinstance(to_date_ast, datetime)

        found_timestamps.append((from_date_ast.isoformat(), to_date_ast.isoformat()))

        return QueryResult({"data": []}, {})

    body = {
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
            ("timestamp", ">=", "2019-09-18T10:00:00"),
            ("timestamp", "<", "2019-09-19T12:00:00"),
            ("project_id", "IN", [1]),
        ],
        "limit": 10,
        "orderby": ["-timestamp"],
    }

    query = parse_query(body, get_dataset("events"))
    entity = get_entity(EntityKey(query.get_entity_name()))
    settings = HTTPRequestSettings()
    for p in entity.get_query_processors():
        p.process_query(query, settings)

    splitter = TimeSplitQueryStrategy("timestamp")
    splitter.execute(ClickhouseQuery(query), settings, do_query)

    assert found_timestamps == [
        ("2019-09-19T11:00:00", "2019-09-19T12:00:00"),
        ("2019-09-19T01:00:00", "2019-09-19T11:00:00"),
        ("2019-09-18T10:00:00", "2019-09-19T01:00:00"),
    ]
