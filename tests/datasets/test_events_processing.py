from snuba.clickhouse.query import Query
from snuba.clickhouse.sql import SqlQuery
from snuba.datasets.factory import get_dataset
from snuba.query.expressions import Column, FunctionCall, Literal
from snuba.query.parser import parse_query
from snuba.reader import Reader
from snuba.request import Request
from snuba.request.request_settings import HTTPRequestSettings, RequestSettings
from snuba.web import QueryResult


def test_events_processing() -> None:
    query_body = {"selected_columns": ["tags[transaction]", "contexts[browser.name]"]}

    events = get_dataset("events")
    query = parse_query(query_body, events)
    request = Request("", query, HTTPRequestSettings(), {}, "")

    query_plan = events.get_query_plan_builder().build_plan(request)
    for clickhouse_processor in query_plan.plan_processors:
        clickhouse_processor.process_query(query_plan.query, request.settings)

    def query_runner(
        query: Query, settings: RequestSettings, reader: Reader[SqlQuery]
    ) -> QueryResult:
        assert query.get_selected_columns_from_ast() == [
            Column("tags[transaction]", None, "transaction"),
            FunctionCall(
                "contexts[browser.name]",
                "arrayElement",
                (
                    Column(None, None, "contexts.value"),
                    FunctionCall(
                        None,
                        "indexOf",
                        (
                            Column(None, None, "contexts.key"),
                            Literal(None, "browser.name"),
                        ),
                    ),
                ),
            ),
        ]
        return QueryResult({}, {})

    query_plan.execution_strategy.execute(
        query_plan.query, request.settings, query_runner
    )
