from functools import partial

import pytest

from snuba.clickhouse.query import Query
from snuba.datasets.entities import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.entity import Entity
from snuba.datasets.factory import get_dataset
from snuba.query import SelectedExpression
from snuba.query.expressions import Column, FunctionCall, Literal, StringifyVisitor
from snuba.reader import Reader
from snuba.request import Language
from snuba.request.request_settings import HTTPRequestSettings, RequestSettings
from snuba.request.schema import RequestSchema
from snuba.request.validation import build_request, parse_snql_query
from snuba.utils.metrics.timer import Timer
from snuba.web import QueryResult

span_id_hex = "1337dedbeef42069"
span_id_as_uint64 = int(span_id_hex, 16)


@pytest.mark.parametrize(
    "dataset_name, query_str, entity, expected_table_name",
    [
        pytest.param(
            "discover",
            f"""MATCH (discover)
            SELECT
                contexts[trace.span_id]
            WHERE
                timestamp >= toDateTime('2021-07-25T15:02:10') AND
                timestamp < toDateTime('2021-07-26T15:02:10') AND
                contexts[trace.span_id] = '{span_id_hex}' AND
                project_id IN tuple(5492900)
            """,
            get_entity(EntityKey.DISCOVER),
            "discover_local",
            id="discover",
        ),
        pytest.param(
            "discover",
            f"""MATCH (discover)
            SELECT
                contexts[trace.span_id]
            WHERE
                timestamp >= toDateTime('2021-07-25T15:02:10') AND
                timestamp < toDateTime('2021-07-26T15:02:10') AND
                contexts[trace.span_id] = '{span_id_hex}' AND
                project_id IN tuple(5492900) AND
                group_id IN tuple(1234)
            """,
            get_entity(EntityKey.EVENTS),
            "errors_local",
            id="events",
        ),
    ],
)
def test_span_id_promotion(
    dataset_name: str, query_str: str, entity: Entity, expected_table_name: str
) -> None:
    query_body = {
        "query": query_str,
        "debug": True,
        "dataset": dataset_name,
        "turbo": False,
        "consistent": False,
    }

    dataset = get_dataset(dataset_name)
    parser = partial(parse_snql_query, [])

    schema = RequestSchema.build_with_extensions(
        entity.get_extensions(), HTTPRequestSettings, Language.SNQL,
    )

    request = build_request(
        query_body,
        parser,
        HTTPRequestSettings,
        schema,
        dataset,
        Timer(name="bloop"),
        "some_referrer",
    )

    def query_verifier(
        query: Query, settings: RequestSettings, reader: Reader
    ) -> QueryResult:
        assert query.get_from_clause().table_name == expected_table_name
        assert query.get_selected_columns() == [
            SelectedExpression(
                name="contexts[trace.span_id]",
                expression=FunctionCall(
                    "_snuba_contexts[trace.span_id]",
                    "lower",
                    (FunctionCall(None, "hex", (Column(None, None, "span_id"),)),),
                ),
            )
        ]

        class SpanIdVerifier(StringifyVisitor):
            def __init__(self) -> None:
                self.found_span_condition = False
                super().__init__()

            def visit_function_call(self, exp: FunctionCall) -> str:
                if exp.function_name == "equals" and exp.parameters[0] == Column(
                    None, None, "span_id"
                ):
                    self.found_span_condition = True
                    assert exp.parameters[1] == Literal(None, span_id_as_uint64)
                return super().visit_function_call(exp)

        verifier = SpanIdVerifier()
        condition = query.get_condition()
        assert condition is not None
        condition.accept(verifier)
        assert verifier.found_span_condition

    entity.get_query_pipeline_builder().build_execution_pipeline(
        request, query_verifier
    ).execute()
