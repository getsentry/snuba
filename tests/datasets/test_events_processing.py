from snuba.attribution import get_app_id
from snuba.attribution.attribution_info import AttributionInfo
from snuba.clickhouse.query import Query
from snuba.datasets.factory import get_dataset
from snuba.query import SelectedExpression
from snuba.query.expressions import Column, FunctionCall, Literal
from snuba.query.query_settings import HTTPQuerySettings, QuerySettings
from snuba.query.snql.parser import parse_snql_query
from snuba.reader import Reader
from snuba.request import Request
from snuba.web import QueryResult


def test_events_processing() -> None:
    query_body = {
        "query": """
        MATCH (events)
        SELECT tags[transaction], contexts[browser.name]
        WHERE project_id = 1
        AND timestamp >= toDateTime('2020-01-01 12:00:00')
        AND timestamp < toDateTime('2020-01-02 12:00:00')
        """,
        "dataset": "events",
    }

    events_dataset = get_dataset("events")
    events_entity = events_dataset.get_default_entity()

    query, snql_anonymized = parse_snql_query(query_body["query"], events_dataset)
    request = Request(
        id="",
        original_body=query_body,
        query=query,
        snql_anonymized=snql_anonymized,
        query_settings=HTTPQuerySettings(referrer=""),
        attribution_info=AttributionInfo(get_app_id("blah"), "blah", None, None, None),
    )

    def query_runner(
        query: Query, settings: QuerySettings, reader: Reader
    ) -> QueryResult:
        assert query.get_selected_columns() == [
            SelectedExpression(
                "tags[transaction]",
                Column("_snuba_tags[transaction]", None, "transaction_name"),
            ),
            SelectedExpression(
                "contexts[browser.name]",
                FunctionCall(
                    "_snuba_contexts[browser.name]",
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

    events_entity.get_query_pipeline_builder().build_execution_pipeline(
        request, query_runner
    ).execute()
