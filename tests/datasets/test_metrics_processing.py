import importlib

from snuba import settings
from snuba.clickhouse.query import Query
from snuba.datasets import factory
from snuba.datasets.entities import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.factory import get_dataset
from snuba.datasets.storages import factory as storage_factory
from snuba.query import SelectedExpression
from snuba.query.expressions import Column, FunctionCall, Literal
from snuba.query.snql.parser import parse_snql_query
from snuba.reader import Reader
from snuba.request import Request
from snuba.request.request_settings import HTTPRequestSettings, RequestSettings
from snuba.web import QueryResult


def test_metrics_processing() -> None:
    settings.ENABLE_DEV_FEATURES = True
    settings.DISABLED_DATASETS = set()

    importlib.reload(factory)
    importlib.reload(storage_factory)

    query_body = {
        "query": (
            "MATCH (metrics_sets) "
            "SELECT value BY org_id, project_id, tags[10] "
            "WHERE "
            "timestamp >= toDateTime('2021-05-17 19:42:01') AND "
            "timestamp < toDateTime('2021-05-17 23:42:01') AND "
            "org_id = 1 AND "
            "project_id = 1"
        ),
    }

    metrics_dataset = get_dataset("metrics")
    query = parse_snql_query(query_body["query"], [], metrics_dataset)

    request = Request("", query_body, query, HTTPRequestSettings(), "")

    def query_runner(
        query: Query, settings: RequestSettings, reader: Reader
    ) -> QueryResult:
        assert query.get_selected_columns() == [
            SelectedExpression("org_id", Column("_snuba_org_id", None, "org_id"),),
            SelectedExpression(
                "project_id", Column("_snuba_project_id", None, "project_id"),
            ),
            SelectedExpression(
                "tags[10]",
                FunctionCall(
                    "_snuba_tags[10]",
                    "arrayElement",
                    (
                        Column(None, None, "tags.value"),
                        FunctionCall(
                            None,
                            "indexOf",
                            (Column(None, None, "tags.key"), Literal(None, 10),),
                        ),
                    ),
                ),
            ),
            SelectedExpression(
                "value",
                FunctionCall(
                    "_snuba_value",
                    "uniqCombined64Merge",
                    (Column(None, None, "value"),),
                ),
            ),
        ]
        return QueryResult({}, {})

    entity = get_entity(EntityKey.METRICS_SETS)
    entity.get_query_pipeline_builder().build_execution_pipeline(
        request, query_runner
    ).execute()
