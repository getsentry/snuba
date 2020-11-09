from snuba.clickhouse.query import Query
from snuba.datasets.factory import get_dataset
from snuba.pipeline.single_query_plan_pipeline import SingleQueryPlanPipeline
from snuba.query import SelectedExpression
from snuba.query.expressions import Column, CurriedFunctionCall, FunctionCall, Literal
from snuba.query.parser import parse_query
from snuba.reader import Reader
from snuba.request import Request
from snuba.request.request_settings import HTTPRequestSettings, RequestSettings
from snuba.web import QueryResult


def test_sessions_processing() -> None:
    query_body = {"selected_columns": ["duration_quantiles", "sessions", "users"]}

    sessions = get_dataset("sessions")
    query = parse_query(query_body, sessions)
    request = Request("", query, HTTPRequestSettings(), {}, "")

    def query_runner(
        query: Query, settings: RequestSettings, reader: Reader
    ) -> QueryResult:
        assert query.get_selected_columns_from_ast() == [
            SelectedExpression(
                "duration_quantiles",
                CurriedFunctionCall(
                    "duration_quantiles",
                    FunctionCall(
                        None,
                        "quantilesIfMerge",
                        (Literal(None, 0.5), Literal(None, 0.9)),
                    ),
                    (Column(None, None, "duration_quantiles"),),
                ),
            ),
            SelectedExpression(
                "sessions",
                FunctionCall(
                    "sessions", "countIfMerge", (Column(None, None, "sessions"),)
                ),
            ),
            SelectedExpression(
                "users",
                FunctionCall("users", "uniqIfMerge", (Column(None, None, "users"),)),
            ),
        ]
        return QueryResult({}, {})

    pipeline = (
        sessions.get_default_entity()
        .get_query_pipeline_builder()
        .build_pipeline(request, query_runner)
    )
    assert isinstance(pipeline, SingleQueryPlanPipeline)
    query_plan = pipeline.query_plan

    query_plan.execution_strategy.execute(
        query_plan.query, request.settings, query_runner
    )
