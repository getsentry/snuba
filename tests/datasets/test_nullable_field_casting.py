import pytest

from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.entity import Entity
from snuba.datasets.factory import get_dataset
from snuba.pipeline.query_pipeline import QueryPipelineResult
from snuba.pipeline.stages.query_processing import (
    EntityProcessingStage,
    StorageProcessingStage,
)
from snuba.query.expressions import Column, FunctionCall, Literal, StringifyVisitor
from snuba.query.query_settings import HTTPQuerySettings
from snuba.request.schema import RequestSchema
from snuba.request.validation import build_request, parse_snql_query
from snuba.utils.metrics.timer import Timer


@pytest.mark.parametrize(
    "entity, expected_table_name",
    [
        pytest.param(
            get_entity(EntityKey.DISCOVER),
            "discover",
            id="discover",
        )
    ],
)
@pytest.mark.clickhouse_db
def test_nullable_field_casting(entity: Entity, expected_table_name: str) -> None:
    dataset_name = "discover"

    query_str = """MATCH (discover)
    SELECT
        uniq(sdk_version)
    WHERE
        timestamp >= toDateTime('2021-07-25T15:02:10') AND
        timestamp < toDateTime('2021-07-26T15:02:10') AND
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

    schema = RequestSchema.build(HTTPQuerySettings)

    request = build_request(
        query_body,
        parse_snql_query,
        HTTPQuerySettings,
        schema,
        dataset,
        Timer(name="bloop"),
        "some_referrer",
    )
    pipeline_result = EntityProcessingStage().execute(
        QueryPipelineResult(
            data=request,
            query_settings=request.query_settings,
            timer=Timer(name="bloop"),
            error=None,
        )
    )
    clickhouse_query = StorageProcessingStage().execute(pipeline_result).data

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

    for select_expr in clickhouse_query.get_selected_columns():
        verifier = NullCastingVerifier()
        select_expr.expression.accept(verifier)
        assert verifier.sdk_version_cast_to_null
