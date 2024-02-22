from datetime import datetime
from typing import Any, MutableMapping, Sequence

import pytest
from snuba_sdk.legacy import json_to_snql

from snuba import state
from snuba.clickhouse.columns import ColumnSet, String
from snuba.clickhouse.query import Query as ClickhouseQuery
from snuba.clickhouse.query_dsl.accessors import get_time_range
from snuba.clusters.cluster import ClickhouseCluster
from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.factory import get_dataset
from snuba.datasets.plans.splitters.strategies import (
    ColumnSplitQueryStrategy,
    TimeSplitQueryStrategy,
)
from snuba.datasets.plans.storage_plan_builder import SimpleQueryPlanExecutionStrategy
from snuba.datasets.plans.translator.query import identity_translate
from snuba.query import SelectedExpression
from snuba.query.data_source.simple import Table
from snuba.query.expressions import Column
from snuba.query.query_settings import HTTPQuerySettings, QuerySettings
from snuba.query.snql.parser import parse_snql_query
from snuba.reader import Reader
from snuba.web import QueryResult


@pytest.fixture(autouse=True)
def setup_teardown(redis_db: None) -> None:
    state.set_config("use_split", 1)


split_specs = [
    (
        "events",
        "events",
        "event_id",
        "project_id",
        "timestamp",
    ),
    (
        "transactions",
        "transactions",
        "event_id",
        "project_id",
        "finish_ts",
    ),
]


@pytest.mark.parametrize(
    "dataset_name, entity_name, id_column, project_column, timestamp_column",
    split_specs,
)
@pytest.mark.clickhouse_db
@pytest.mark.redis_db
def test_no_split(
    dataset_name: str,
    entity_name: str,
    id_column: str,
    project_column: str,
    timestamp_column: str,
) -> None:
    entity_key = EntityKey(entity_name)
    entity = get_entity(entity_key)
    query = ClickhouseQuery(
        entity.get_all_storages()[0].get_schema().get_data_source(),
    )

    def do_query(
        clickhouse_query: ClickhouseQuery,
        query_settings: QuerySettings,
        reader: Reader,
        cluster_name: str,
    ) -> QueryResult:
        assert query == query
        return QueryResult({}, {})

    strategy = SimpleQueryPlanExecutionStrategy(
        ClickhouseCluster("127.0.0.1", 1024, "default", "", "default", 80, set(), True),
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

    strategy.execute(query, HTTPQuerySettings(), do_query)


@pytest.mark.redis_db
def test_set_limit_on_split_query():
    storage = get_entity(EntityKey.EVENTS).get_all_storages()[0]
    query = ClickhouseQuery(
        Table("events", storage.get_schema().get_columns()),
        selected_columns=[
            SelectedExpression(col.name, Column(None, None, col.name))
            for col in storage.get_schema().get_columns()
        ],
        limit=420,
    )

    query_run_count = 0

    def do_query(
        clickhouse_query: ClickhouseQuery, query_settings: QuerySettings
    ) -> QueryResult:
        nonlocal query_run_count
        query_run_count += 1
        if query_run_count == 1:
            return QueryResult(
                result={
                    "data": [
                        {
                            "event_id": "a",
                            "project_id": "1",
                            "timestamp": " 2019-10-01 22:33:42",
                        },
                        {
                            "event_id": "a",
                            "project_id": "1",
                            "timestamp": " 2019-10-01 22:44:42",
                        },
                    ]
                },
                extra={},
            )
        else:
            assert clickhouse_query.get_limit() == 2
            return QueryResult({}, {})

    ColumnSplitQueryStrategy(
        id_column="event_id",
        project_column="project_id",
        timestamp_column="timestamp",
    ).execute(query, HTTPQuerySettings(), do_query)
    assert query_run_count == 2


test_data_col = [
    (
        "events",
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
        "transactions",
        "transactions",
        "event_id",
        "project_id",
        "finish_ts",
        [{"event_id": "a", "project_id": "1", "finish_ts": "2019-10-01 22:33:42"}],
        [
            {
                "event_id": "a",
                "project_id": "1",
                "level": "error",
                "finish_ts": "2019-10-01 22:33:42",
            }
        ],
    ),
]


@pytest.mark.parametrize(
    "dataset_name, entity_name, id_column, project_column, timestamp_column, first_query_data, second_query_data",
    test_data_col,
)
@pytest.mark.clickhouse_db
@pytest.mark.redis_db
def test_col_split(
    dataset_name: str,
    entity_name: str,
    id_column: str,
    project_column: str,
    timestamp_column: str,
    first_query_data: Sequence[MutableMapping[str, Any]],
    second_query_data: Sequence[MutableMapping[str, Any]],
) -> None:
    def do_query(
        clickhouse_query: ClickhouseQuery,
        query_settings: QuerySettings,
        reader: Reader,
        cluster_name: str,
    ) -> QueryResult:
        selected_col_names = [
            c.expression.column_name
            for c in query.get_selected_columns() or []
            if isinstance(c.expression, Column)
        ]
        if selected_col_names == list(first_query_data[0].keys()):
            return QueryResult({"data": first_query_data}, {})
        elif selected_col_names == list(second_query_data[0].keys()):
            return QueryResult({"data": second_query_data}, {})
        else:
            raise ValueError(f"Unexpected selected columns: {selected_col_names}")

    entity_key = EntityKey(entity_name)
    entity = get_entity(entity_key)
    query = ClickhouseQuery(
        entity.get_all_storages()[0].get_schema().get_data_source(),
        selected_columns=[
            SelectedExpression(name=col_name, expression=Column(None, None, col_name))
            for col_name in second_query_data[0].keys()
        ],
    )

    strategy = SimpleQueryPlanExecutionStrategy(
        ClickhouseCluster("127.0.0.1", 1024, "default", "", "default", 80, set(), True),
        [],
        [
            ColumnSplitQueryStrategy(id_column, project_column, timestamp_column),
            TimeSplitQueryStrategy(timestamp_col=timestamp_column),
        ],
    )

    strategy.execute(query, HTTPQuerySettings(), do_query)


column_set = ColumnSet(
    [
        ("event_id", String()),
        ("project_id", String()),
        ("timestamp", String()),
        ("level", String()),
        ("release", String()),
        ("platform", String()),
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
                "release",
                "platform",
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
                "release",
                "platform",
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
            "selected_columns": ["event_id", "level", "release", "platform"],
            "conditions": [
                ("timestamp", ">=", "2019-09-19T10:00:00"),
                ("timestamp", "<", "2019-09-19T12:00:00"),
                ("project_id", "IN", [1, 2, 3]),
            ],
            "limit": 10,
        },
        False,
    ),  # Valid query but not enough columns in the select
    (
        "event_id",
        "project_id",
        "timestamp",
        {
            "selected_columns": [
                "event_id",
                "level",
                "release",
                "platform",
                "transaction",
                "timestamp",
                "project_id",
            ],
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
            "selected_columns": [
                "event_id",
                "level",
                "release",
                "platform",
                "transaction",
                "timestamp",
                "project_id",
            ],
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
            "selected_columns": [
                "event_id",
                "level",
                "release",
                "platform",
                "transaction",
                "timestamp",
                "project_id",
            ],
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
@pytest.mark.redis_db
def test_col_split_conditions(
    id_column: str, project_column: str, timestamp_column: str, query, expected_result
) -> None:
    dataset = get_dataset("events")
    request = json_to_snql(query, "events")
    request.validate()
    query, _ = parse_snql_query(str(request.query), dataset)
    splitter = ColumnSplitQueryStrategy(id_column, project_column, timestamp_column)

    def do_query(
        query: ClickhouseQuery, query_settings: QuerySettings = None
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
        splitter.execute(query, HTTPQuerySettings(), do_query) is not None
    ) == expected_result


@pytest.mark.redis_db
def test_time_split_ast() -> None:
    """
    Test that the time split transforms the query properly both on the old representation
    and on the AST representation.
    """
    found_timestamps = []

    def do_query(
        query: ClickhouseQuery,
        query_settings: QuerySettings,
    ) -> QueryResult:
        from_date_ast, to_date_ast = get_time_range(query, "timestamp")
        assert from_date_ast is not None and isinstance(from_date_ast, datetime)
        assert to_date_ast is not None and isinstance(to_date_ast, datetime)

        found_timestamps.append((from_date_ast.isoformat(), to_date_ast.isoformat()))

        return QueryResult({"data": []}, {})

    body = """
        MATCH (events)
        SELECT event_id, level, offset, partition, transaction, timestamp, project_id
        WHERE timestamp >= toDateTime('2019-09-18T10:00:00')
        AND timestamp < toDateTime('2019-09-19T12:00:00')
        AND project_id IN tuple(1)
        ORDER BY timestamp DESC
        LIMIT 10
        """

    query, _ = parse_snql_query(body, get_dataset("events"))
    entity = get_entity(query.get_from_clause().key)
    settings = HTTPQuerySettings()
    for p in entity.get_query_processors():
        p.process_query(query, settings)

    clickhouse_query = identity_translate(query)
    splitter = TimeSplitQueryStrategy("timestamp")
    splitter.execute(clickhouse_query, settings, do_query)

    assert found_timestamps == [
        ("2019-09-19T11:00:00", "2019-09-19T12:00:00"),
        ("2019-09-19T01:00:00", "2019-09-19T11:00:00"),
        ("2019-09-18T10:00:00", "2019-09-19T01:00:00"),
    ]
