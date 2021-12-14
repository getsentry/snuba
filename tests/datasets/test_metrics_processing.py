import importlib
from typing import Union

import pytest

from snuba import settings
from snuba.clickhouse.query import Expression, Query
from snuba.clusters import cluster
from snuba.datasets import factory
from snuba.datasets.entities import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.factory import get_dataset
from snuba.datasets.storages import factory as storage_factory
from snuba.query import SelectedExpression
from snuba.query.composite import CompositeQuery
from snuba.query.data_source.simple import Table
from snuba.query.expressions import Column, CurriedFunctionCall, FunctionCall, Literal
from snuba.query.snql.parser import parse_snql_query
from snuba.reader import Reader
from snuba.request import Request
from snuba.request.request_settings import HTTPRequestSettings, RequestSettings
from snuba.web import QueryResult

TEST_CASES = [
    pytest.param(
        "metrics_sets",
        "uniq(value)",
        EntityKey.METRICS_SETS,
        FunctionCall(
            None, "uniqCombined64Merge", (Column("_snuba_value", None, "value"),),
        ),
        id="Test sets entity",
    ),
    pytest.param(
        "metrics_counters",
        "sum(value)",
        EntityKey.METRICS_COUNTERS,
        FunctionCall(None, "sumMerge", (Column("_snuba_value", None, "value"),),),
        id="Test counters entity",
    ),
    pytest.param(
        "metrics_counters",
        "sumIf(value, cond())",
        EntityKey.METRICS_COUNTERS,
        FunctionCall(
            None,
            "sumMergeIf",
            (
                Column("_snuba_value", None, "value"),
                FunctionCall(None, "cond", tuple()),
            ),
        ),
        id="Test counters entity with mergeIf",
    ),
    pytest.param(
        "metrics_distributions",
        "max(value)",
        EntityKey.METRICS_DISTRIBUTIONS,
        FunctionCall(None, "maxMerge", (Column(None, None, "max"),),),
        id="Test distribution max",
    ),
    pytest.param(
        "metrics_distributions",
        "maxIf(value, cond())",
        EntityKey.METRICS_DISTRIBUTIONS,
        FunctionCall(
            None,
            "maxMergeIf",
            (Column(None, None, "max"), FunctionCall(None, "cond", tuple()),),
        ),
        id="Test distribution max with condition",
    ),
    pytest.param(
        "metrics_distributions",
        "min(value)",
        EntityKey.METRICS_DISTRIBUTIONS,
        FunctionCall(None, "minMerge", (Column(None, None, "min"),),),
        id="Test distribution min",
    ),
    pytest.param(
        "metrics_distributions",
        "minIf(value, cond())",
        EntityKey.METRICS_DISTRIBUTIONS,
        FunctionCall(
            None,
            "minMergeIf",
            (Column(None, None, "min"), FunctionCall(None, "cond", tuple()),),
        ),
        id="Test distribution min with condition",
    ),
    pytest.param(
        "metrics_distributions",
        "avg(value)",
        EntityKey.METRICS_DISTRIBUTIONS,
        FunctionCall(None, "avgMerge", (Column(None, None, "avg"),),),
        id="Test distribution avg",
    ),
    pytest.param(
        "metrics_distributions",
        "avgIf(value, cond())",
        EntityKey.METRICS_DISTRIBUTIONS,
        FunctionCall(
            None,
            "avgMergeIf",
            (Column(None, None, "avg"), FunctionCall(None, "cond", tuple()),),
        ),
        id="Test distribution avg with condition",
    ),
    pytest.param(
        "metrics_distributions",
        "count(value)",
        EntityKey.METRICS_DISTRIBUTIONS,
        FunctionCall(None, "countMerge", (Column(None, None, "count"),),),
        id="Test distribution count",
    ),
    pytest.param(
        "metrics_distributions",
        "quantiles(0.5, 0.75, 0.9, 0.95, 0.99)(value)",
        EntityKey.METRICS_DISTRIBUTIONS,
        CurriedFunctionCall(
            None,
            FunctionCall(
                None,
                "quantilesMerge",
                tuple(Literal(None, quant) for quant in [0.5, 0.75, 0.9, 0.95, 0.99]),
            ),
            (Column(None, None, "percentiles"),),
        ),
        id="Test distribution quantiles",
    ),
    pytest.param(
        "metrics_distributions",
        "quantilesIf(0.5, 0.75, 0.9, 0.95, 0.99)(value, cond())",
        EntityKey.METRICS_DISTRIBUTIONS,
        CurriedFunctionCall(
            None,
            FunctionCall(
                None,
                "quantilesMergeIf",
                tuple(Literal(None, quant) for quant in [0.5, 0.75, 0.9, 0.95, 0.99]),
            ),
            (Column(None, None, "percentiles"), FunctionCall(None, "cond", tuple())),
        ),
        id="Test distribution quantiles",
    ),
    pytest.param(
        "metrics_distributions",
        "avg(something_else)",
        EntityKey.METRICS_DISTRIBUTIONS,
        FunctionCall(
            None, "avg", (Column("_snuba_something_else", None, "something_else"),),
        ),
        id="Test that a column other than value is not transformed",
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
    importlib.reload(cluster)

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
    query, snql_anonymized = parse_snql_query(query_body["query"], metrics_dataset)

    request = Request(
        id="",
        body=query_body,
        query=query,
        snql_anonymized="",
        settings=HTTPRequestSettings(referrer=""),
    )

    def query_runner(
        query: Union[Query, CompositeQuery[Table]],
        settings: RequestSettings,
        reader: Reader,
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
        return QueryResult(
            result={"meta": [], "data": [], "totals": {}},
            extra={"stats": {}, "sql": "", "experiments": {}},
        )

    entity = get_entity(entity_key)
    entity.get_query_pipeline_builder().build_execution_pipeline(
        request, query_runner
    ).execute()
