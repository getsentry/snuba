import importlib

import pytest

from snuba import settings
from snuba.clickhouse.query import Expression, Query
from snuba.datasets import factory
from snuba.datasets.entities import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.factory import get_dataset
from snuba.datasets.storages import factory as storage_factory
from snuba.query import SelectedExpression
from snuba.query.expressions import Column, CurriedFunctionCall, FunctionCall, Literal
from snuba.query.snql.parser import parse_snql_query
from snuba.reader import Reader
from snuba.request import Request
from snuba.request.request_settings import HTTPRequestSettings, RequestSettings
from snuba.web import QueryResult

TEST_CASES = [
    pytest.param(
        "metrics_sets",
        "value",
        EntityKey.METRICS_SETS,
        FunctionCall(
            "_snuba_value", "uniqCombined64Merge", (Column(None, None, "value"),),
        ),
        id="Test sets entity",
    ),
    pytest.param(
        "metrics_counters",
        "value",
        EntityKey.METRICS_COUNTERS,
        FunctionCall("_snuba_value", "sumMerge", (Column(None, None, "value"),),),
        id="Test counters entity",
    ),
    pytest.param(
        "metrics_distributions",
        "max",
        EntityKey.METRICS_DISTRIBUTIONS,
        FunctionCall("_snuba_max", "maxMerge", (Column(None, None, "max"),),),
        id="Test distribution max",
    ),
    pytest.param(
        "metrics_distributions",
        "min",
        EntityKey.METRICS_DISTRIBUTIONS,
        FunctionCall("_snuba_min", "minMerge", (Column(None, None, "min"),),),
        id="Test distribution min",
    ),
    pytest.param(
        "metrics_distributions",
        "avg",
        EntityKey.METRICS_DISTRIBUTIONS,
        FunctionCall("_snuba_avg", "avgMerge", (Column(None, None, "avg"),),),
        id="Test distribution avg",
    ),
    pytest.param(
        "metrics_distributions",
        "count",
        EntityKey.METRICS_DISTRIBUTIONS,
        FunctionCall("_snuba_count", "countMerge", (Column(None, None, "count"),),),
        id="Test distribution count",
    ),
    pytest.param(
        "metrics_distributions",
        "percentiles",
        EntityKey.METRICS_DISTRIBUTIONS,
        CurriedFunctionCall(
            "_snuba_percentiles",
            FunctionCall(
                None,
                "quantilesMerge",
                tuple(
                    Literal(None, quant) for quant in [0.5, 0.75, 0.9, 0.95, 0.99, 1]
                ),
            ),
            (Column(None, None, "percentiles"),),
        ),
        id="Test distribution quantiles",
    ),
]


@pytest.mark.parametrize(
    "entity_name, column_name, entity_key, translated_value", TEST_CASES
)
def test_metrics_processing(
    entity_name: str,
    column_name: str,
    entity_key: EntityKey,
    translated_value: Expression,
) -> None:
    settings.ENABLE_DEV_FEATURES = True
    settings.DISABLED_DATASETS = set()

    importlib.reload(factory)
    importlib.reload(storage_factory)

    query_body = {
        "query": (
            f"MATCH ({entity_name}) "
            f"SELECT {column_name} BY org_id, project_id, tags[10] "
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
                            (Column(None, None, "tags.key"), Literal(None, 10)),
                        ),
                    ),
                ),
            ),
            SelectedExpression(column_name, translated_value,),
        ]
        return QueryResult({}, {})

    entity = get_entity(entity_key)
    entity.get_query_pipeline_builder().build_execution_pipeline(
        request, query_runner
    ).execute()
