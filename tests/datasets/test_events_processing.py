from snuba.clickhouse.query import Query
from snuba.clickhouse.sql import SqlQuery
from snuba.datasets.factory import get_dataset
from snuba.query import SelectedExpression
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

    query_pipeline = (
        events.get_default_entity().get_query_pipeline_builder().build_pipeline(request)
    )

    def query_runner(
        query: Query, settings: RequestSettings, reader: Reader[SqlQuery]
    ) -> QueryResult:
        assert query.get_selected_columns_from_ast() == [
            SelectedExpression(
                "tags[transaction]", Column("tags[transaction]", None, "transaction")
            ),
            SelectedExpression(
                "contexts[browser.name]",
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
            ),
        ]
        return QueryResult({}, {})

    query_pipeline.execute(query_runner)
