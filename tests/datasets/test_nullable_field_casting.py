from functools import partial

import pytest

from snuba.clickhouse.query import Query
from snuba.datasets.entities import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.entity import Entity
from snuba.datasets.factory import get_dataset
from snuba.query.expressions import Column, FunctionCall, Literal, StringifyVisitor
from snuba.reader import Reader
from snuba.request import Language
from snuba.request.request_settings import HTTPRequestSettings, RequestSettings
from snuba.request.schema import RequestSchema
from snuba.request.validation import build_request, parse_snql_query
from snuba.utils.metrics.timer import Timer

span_id_hex = "1337dedbeef42069"
span_id_as_uint64 = int(span_id_hex, 16)


@pytest.mark.parametrize(
    "entity, expected_table_name",
    [pytest.param(get_entity(EntityKey.DISCOVER), "discover", id="discover",)],
)
def test_span_id_promotion(entity: Entity, expected_table_name: str) -> None:
    dataset_name = "discover"

    # The client queries by contexts[trace.span_id] even though that's not how we store it
    query_str = f"""MATCH (discover)
    SELECT
        uniq(sdk_version)
    WHERE
        timestamp >= toDateTime('2021-07-25T15:02:10') AND
        timestamp < toDateTime('2021-07-26T15:02:10') AND
        contexts[trace.span_id] = '{span_id_hex}' AND
        project_id IN tuple(5492900)
    """

    # ----- create the request object as if it came in through our API -----
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
    # --------------------------------------------------------------------

    def query_verifier(query: Query, settings: RequestSettings, reader: Reader) -> None:
        # The only reason this extends StringifyVisitor is because it has all the other
        # visit methods implemented.
        class NullCastingVerifier(StringifyVisitor):
            def __init__(self) -> None:
                self.sdk_version_cast_to_null = False
                super().__init__()

            def visit_function_call(self, exp: FunctionCall) -> str:
                if (
                    exp.function_name == "cast"
                    and exp.alias == "_snuba_sdk_version"
                    and exp.parameters
                    == (
                        Column(None, None, "sdk_version"),
                        Literal(None, "Nullable(String)"),
                    )
                ):
                    self.sdk_version_cast_to_null = True
                return super().visit_function_call(exp)

        for select_expr in query.get_selected_columns():
            verifier = NullCastingVerifier()
            select_expr.expression.accept(verifier)
            assert verifier.sdk_version_cast_to_null

    entity.get_query_pipeline_builder().build_execution_pipeline(
        request, query_verifier
    ).execute()
