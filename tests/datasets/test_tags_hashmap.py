import pytest

from snuba.clickhouse.query import Query
from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.factory import get_dataset
from snuba.query.expressions import Column, FunctionCall, NoopVisitor
from snuba.query.query_settings import HTTPQuerySettings, QuerySettings
from snuba.reader import Reader
from snuba.request.schema import RequestSchema
from snuba.request.validation import build_request, parse_snql_query
from snuba.utils.metrics.timer import Timer


@pytest.mark.clickhouse_db
def test_tags_hashmap_optimization() -> None:
    entity = get_entity(EntityKey.DISCOVER)
    dataset_name = "discover"
    query_str = """
    MATCH (discover)
    SELECT count() AS count
    WHERE
        timestamp >= toDateTime('2021-07-12T19:45:01') AND
        timestamp < toDateTime('2021-08-11T19:45:01') AND
        project_id IN tuple(300688)
        AND ifNull(tags[duration_group], '') != '' AND
        ifNull(tags[duration_group], '') = '<10s'
    LIMIT 50
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
    # --------------------------------------------------------------------

    def query_verifier(
        clickhouse_query: Query, query_settings: QuerySettings, reader: Reader
    ) -> None:
        class ConditionVisitor(NoopVisitor):
            def __init__(self) -> None:
                self.found_hashmap_condition = False

            def visit_function_call(self, exp: FunctionCall) -> None:
                assert exp.function_name != "arrayElement"
                if (
                    exp.function_name == "has"
                    and isinstance(exp.parameters[0], Column)
                    and exp.parameters[0].column_name == "_tags_hash_map"
                ):
                    self.found_hashmap_condition = True
                return super().visit_function_call(exp)

        visitor = ConditionVisitor()
        clickhouse_query.get_condition().accept(visitor)
        assert visitor.found_hashmap_condition

    entity.get_query_pipeline_builder().build_execution_pipeline(
        request, query_verifier
    ).execute()
