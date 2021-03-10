from snuba.clickhouse.query import Query
from snuba.datasets.factory import get_dataset
from snuba.datasets.storages import StorageKey
from snuba.query import SelectedExpression
from snuba.query.expressions import Column, FunctionCall, Literal
from snuba.query.parser import parse_query
from snuba.reader import Reader
from snuba.request import Request
from snuba.request.request_settings import HTTPRequestSettings, RequestSettings
from snuba.web import QueryResult


def test_events_processing() -> None:
    query_body = {"selected_columns": ["tags[transaction]", "contexts[browser.name]"]}

    events_dataset = get_dataset("events")
    events_entity = events_dataset.get_default_entity()
    events_storage = events_entity.get_writable_storage()

    query = parse_query(query_body, events_dataset)
    request = Request("", query_body, query, HTTPRequestSettings(), "")

    def query_runner(
        query: Query, settings: RequestSettings, reader: Reader
    ) -> QueryResult:

        if events_storage.get_storage_key() == StorageKey.EVENTS:
            transaction_col_name = "transaction"
        else:
            transaction_col_name = "transaction_name"

        assert query.get_selected_columns() == [
            SelectedExpression(
                "tags[transaction]",
                Column("_snuba_tags[transaction]", None, transaction_col_name),
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
