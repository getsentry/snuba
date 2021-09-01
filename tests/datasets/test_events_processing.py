from snuba.clickhouse.query import Query
from snuba.datasets.factory import get_dataset
from snuba.query import SelectedExpression
from snuba.query.expressions import Column, FunctionCall, Literal
from snuba.query.parser import parse_query
from snuba.reader import Reader
from snuba.request import Request
from snuba.request.request_settings import HTTPRequestSettings, RequestSettings
from snuba.web import QueryResult


def test_events_processing() -> None:
    query_body = {
        "selected_columns": ["tags[transaction]", "contexts[browser.name]"],
        "conditions": [
            ["project_id", "=", 1],
            ["timestamp", ">", "2020-01-01 12:00:00"],
        ],
    }

    events_dataset = get_dataset("events")
    events_entity = events_dataset.get_default_entity()

    query = parse_query(query_body, events_dataset)
    request = Request("", query_body, query, HTTPRequestSettings(referrer=""))

    def query_runner(
        query: Query, settings: RequestSettings, reader: Reader
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


def test_discover_thing() -> None:
    from functools import partial
    from snuba.request.validation import parse_snql_query, build_request
    from snuba.request.schema import RequestSchema
    from snuba.request import Language
    from snuba.utils.metrics.timer import Timer

    query_str = """MATCH (discover)
    SELECT
        contexts[trace.span_id]
    WHERE
        timestamp >= toDateTime('2021-07-25T15:02:10') AND
        timestamp < toDateTime('2021-07-26T15:02:10') AND
        project_id IN tuple(5492900)
    """

    query_body = {
        "query": query_str,
        "debug": True,
        "dataset": "discover",
        "turbo": False,
        "consistent": False,
    }

    discover = get_dataset("discover")
    discover_entity = discover.get_default_entity()
    parser = partial(parse_snql_query, [])

    schema = RequestSchema.build_with_extensions(
        discover_entity.get_extensions(), HTTPRequestSettings, Language.SNQL,
    )

    request = build_request(
        query_body,
        parser,
        HTTPRequestSettings,
        schema,
        discover,
        Timer(name="bloop"),
        "some_referrer",
    )

    def query_runner(
        query: Query, settings: RequestSettings, reader: Reader
    ) -> QueryResult:
        print(query)
        import pdb

        pdb.set_trace()

    discover_entity.get_query_pipeline_builder().build_execution_pipeline(
        request, query_runner
    ).execute()
