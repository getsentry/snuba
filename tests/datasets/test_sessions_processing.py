from snuba.clickhouse.query import Query
from snuba.clickhouse.sql import SqlQuery
from snuba.datasets.factory import get_dataset
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

    query_plan = (
        sessions.get_default_entity().get_query_plan_builder().build_plan(request)
    )
    for clickhouse_processor in query_plan.plan_processors:
        clickhouse_processor.process_query(query_plan.query, request.settings)

    def query_runner(
        query: Query, settings: RequestSettings, reader: Reader[SqlQuery]
    ) -> QueryResult:
        quantiles = tuple(
            Literal(None, quant) for quant in [0.5, 0.75, 0.9, 0.95, 0.99]
        )
        assert query.get_selected_columns_from_ast() == [
            SelectedExpression(
                "duration_quantiles",
                CurriedFunctionCall(
                    "duration_quantiles",
                    FunctionCall(None, "quantilesIfMerge", quantiles,),
                    (Column(None, None, "duration_quantiles"),),
                ),
            ),
            SelectedExpression(
                "sessions",
                FunctionCall(
                    "sessions",
                    "plus",
                    (
                        FunctionCall(
                            None, "countIfMerge", (Column(None, None, "sessions"),)
                        ),
                        FunctionCall(
                            None, "sumIfMerge", (Column(None, None, "sessions_sum"),)
                        ),
                    ),
                ),
            ),
            SelectedExpression(
                "users",
                FunctionCall("users", "uniqIfMerge", (Column(None, None, "users"),)),
            ),
        ]
        return QueryResult({}, {})

    query_plan.execution_strategy.execute(
        query_plan.query, request.settings, query_runner
    )
