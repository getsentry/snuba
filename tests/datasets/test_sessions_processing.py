import pytest
from typing import MutableMapping, Any

from snuba.clickhouse.query import Query
from snuba.datasets.factory import get_dataset
from snuba.query import SelectedExpression
from snuba.query.expressions import Column, CurriedFunctionCall, FunctionCall, Literal
from snuba.query.parser import parse_query
from snuba.reader import Reader
from snuba.request import Request
from snuba.request.request_settings import HTTPRequestSettings, RequestSettings
from snuba.web import QueryResult


def test_sessions_processing() -> None:
    query_body = {
        "selected_columns": ["duration_quantiles", "sessions", "users"],
        "conditions": [
            ["org_id", "=", 1],
            ["project_id", "=", 1],
            ["started", ">", "2020-01-01 12:00:00"],
        ],
    }

    sessions = get_dataset("sessions")
    query = parse_query(query_body, sessions)
    request = Request("", query_body, query, HTTPRequestSettings(), "")

    def query_runner(
        query: Query, settings: RequestSettings, reader: Reader
    ) -> QueryResult:
        quantiles = tuple(
            Literal(None, quant) for quant in [0.5, 0.75, 0.9, 0.95, 0.99, 1]
        )
        assert query.get_selected_columns() == [
            SelectedExpression(
                "duration_quantiles",
                CurriedFunctionCall(
                    "_snuba_duration_quantiles",
                    FunctionCall(None, "quantilesIfMerge", quantiles,),
                    (Column(None, None, "duration_quantiles"),),
                ),
            ),
            SelectedExpression(
                "sessions",
                FunctionCall(
                    "_snuba_sessions",
                    "plus",
                    (
                        FunctionCall(
                            None, "countIfMerge", (Column(None, None, "sessions"),)
                        ),
                        FunctionCall(
                            None,
                            "sumIfMerge",
                            (Column(None, None, "sessions_preaggr"),),
                        ),
                    ),
                ),
            ),
            SelectedExpression(
                "users",
                FunctionCall(
                    "_snuba_users", "uniqIfMerge", (Column(None, None, "users"),)
                ),
            ),
        ]
        return QueryResult({}, {})

    sessions.get_default_entity().get_query_pipeline_builder().build_execution_pipeline(
        request, query_runner
    ).execute()


selector_tests = [
    pytest.param(
        {
            "selected_columns": ["sessions", "bucketed_started"],
            "groupby": ["bucketed_started"],
            "conditions": [
                ["org_id", "=", 1],
                ["project_id", "=", 1],
                ["started", ">", "2020-01-01 12:00:00"],
            ],
        },
        "sessions_hourly_local",
        id="Select hourly by default",
    ),
    pytest.param(
        {
            "selected_columns": ["sessions"],
            "granularity": 60,
            "conditions": [
                ["org_id", "=", 1],
                ["project_id", "=", 1],
                ["started", ">", "2020-01-01 12:00:00"],
            ],
        },
        "sessions_hourly_local",
        id="Select hourly if not grouped by started time",
    ),
    pytest.param(
        {
            "selected_columns": ["sessions", "bucketed_started"],
            "groupby": ["bucketed_started"],
            "granularity": 60,
            "conditions": [
                ("org_id", "=", 1),
                ("project_id", "=", 1),
                ("started", ">=", "2019-09-19T10:00:00"),
                ("started", "<", "2019-09-19T12:00:00"),
            ],
        },
        "sessions_raw_local",
        id="Select raw depending on granularity",
    ),
]


@pytest.mark.parametrize(
    "query_body, expected_table", selector_tests,
)
def test_select_storage(
    query_body: MutableMapping[str, Any], expected_table: str
) -> None:
    sessions = get_dataset("sessions")
    query = parse_query(query_body, sessions)
    request = Request("", query_body, query, HTTPRequestSettings(), "")

    def query_runner(
        query: Query, settings: RequestSettings, reader: Reader
    ) -> QueryResult:
        assert query.get_from_clause().table_name == expected_table
        return QueryResult({}, {})

    sessions.get_default_entity().get_query_pipeline_builder().build_execution_pipeline(
        request, query_runner
    ).execute()
