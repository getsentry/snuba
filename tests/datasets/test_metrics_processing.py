from typing import Union

import pytest

from snuba import settings
from snuba.attribution import get_app_id
from snuba.attribution.attribution_info import AttributionInfo
from snuba.clickhouse.query import Expression, Query
from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.factory import get_dataset
from snuba.query import SelectedExpression
from snuba.query.composite import CompositeQuery
from snuba.query.data_source.simple import Table
from snuba.query.expressions import Column, CurriedFunctionCall, FunctionCall, Literal
from snuba.query.query_settings import HTTPQuerySettings, QuerySettings
from snuba.query.snql.parser import parse_snql_query
from snuba.reader import Reader
from snuba.request import Request
from snuba.web import QueryResult

TEST_CASES = [
    pytest.param(
        "metrics_sets",
        "uniq(value)",
        EntityKey.METRICS_SETS,
        FunctionCall(
            "_snuba_uniq(value)",
            "uniqCombined64Merge",
            (Column("_snuba_value", None, "value"),),
        ),
        id="Test sets entity",
    ),
    pytest.param(
        "metrics_counters",
        "sum(value)",
        EntityKey.METRICS_COUNTERS,
        FunctionCall(
            "_snuba_sum(value)",
            "sumMerge",
            (Column("_snuba_value", None, "value"),),
        ),
        id="Test counters entity",
    ),
    pytest.param(
        "metrics_counters",
        "sumIf(value, cond())",
        EntityKey.METRICS_COUNTERS,
        FunctionCall(
            "_snuba_sumIf(value, cond())",
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
        FunctionCall(
            "_snuba_max(value)",
            "maxMerge",
            (Column(None, None, "max"),),
        ),
        id="Test distribution max",
    ),
    pytest.param(
        "metrics_distributions",
        "maxIf(value, cond())",
        EntityKey.METRICS_DISTRIBUTIONS,
        FunctionCall(
            "_snuba_maxIf(value, cond())",
            "maxMergeIf",
            (
                Column(None, None, "max"),
                FunctionCall(None, "cond", tuple()),
            ),
        ),
        id="Test distribution max with condition",
    ),
    pytest.param(
        "metrics_distributions",
        "min(value)",
        EntityKey.METRICS_DISTRIBUTIONS,
        FunctionCall(
            "_snuba_min(value)",
            "minMerge",
            (Column(None, None, "min"),),
        ),
        id="Test distribution min",
    ),
    pytest.param(
        "metrics_distributions",
        "minIf(value, cond())",
        EntityKey.METRICS_DISTRIBUTIONS,
        FunctionCall(
            "_snuba_minIf(value, cond())",
            "minMergeIf",
            (
                Column(None, None, "min"),
                FunctionCall(None, "cond", tuple()),
            ),
        ),
        id="Test distribution min with condition",
    ),
    pytest.param(
        "metrics_distributions",
        "avg(value)",
        EntityKey.METRICS_DISTRIBUTIONS,
        FunctionCall(
            "_snuba_avg(value)",
            "avgMerge",
            (Column(None, None, "avg"),),
        ),
        id="Test distribution avg",
    ),
    pytest.param(
        "metrics_distributions",
        "avgIf(value, cond())",
        EntityKey.METRICS_DISTRIBUTIONS,
        FunctionCall(
            "_snuba_avgIf(value, cond())",
            "avgMergeIf",
            (
                Column(None, None, "avg"),
                FunctionCall(None, "cond", tuple()),
            ),
        ),
        id="Test distribution avg with condition",
    ),
    pytest.param(
        "metrics_distributions",
        "count(value)",
        EntityKey.METRICS_DISTRIBUTIONS,
        FunctionCall(
            "_snuba_count(value)",
            "countMerge",
            (Column(None, None, "count"),),
        ),
        id="Test distribution count",
    ),
    pytest.param(
        "metrics_distributions",
        "quantiles(0.5, 0.75, 0.9, 0.95, 0.99)(value)",
        EntityKey.METRICS_DISTRIBUTIONS,
        CurriedFunctionCall(
            "_snuba_quantiles(0.5, 0.75, 0.9, 0.95, 0.99)(value)",
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
            "_snuba_quantilesIf(0.5, 0.75, 0.9, 0.95, 0.99)(value, cond())",
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
            "_snuba_avg(something_else)",
            "avg",
            (Column("_snuba_something_else", None, "something_else"),),
        ),
        id="Test that a column other than value is not transformed",
    ),
    pytest.param(
        "metrics_distributions",
        "histogram(250)(value)",
        EntityKey.METRICS_DISTRIBUTIONS,
        CurriedFunctionCall(
            "_snuba_histogram(250)(value)",
            FunctionCall(None, "histogramMerge", (Literal(None, 250),)),
            (Column(None, None, "histogram_buckets"),),
        ),
        id="Test distribution histogram",
    ),
    pytest.param(
        "metrics_distributions",
        "histogramIf(250)(value, cond())",
        EntityKey.METRICS_DISTRIBUTIONS,
        CurriedFunctionCall(
            "_snuba_histogramIf(250)(value, cond())",
            FunctionCall(None, "histogramMergeIf", (Literal(None, 250),)),
            (
                Column(None, None, "histogram_buckets"),
                FunctionCall(None, "cond", tuple()),
            ),
        ),
        id="Test distribution histogram",
    ),
]


@pytest.mark.parametrize(
    "entity_name, column_name, entity_key, translated_value", TEST_CASES
)
@pytest.mark.clickhouse_db
def test_metrics_processing(
    entity_name: str,
    column_name: str,
    entity_key: EntityKey,
    translated_value: Expression,
) -> None:
    settings.ENABLE_DEV_FEATURES = True
    settings.DISABLED_DATASETS = set()

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
        original_body=query_body,
        query=query,
        snql_anonymized="",
        query_settings=HTTPQuerySettings(referrer=""),
        attribution_info=AttributionInfo(
            get_app_id("blah"), {"tenant_type": "tenant_id"}, "blah", None, None, None
        ),
    )

    def query_runner(
        clickhouse_query: Union[Query, CompositeQuery[Table]],
        query_settings: QuerySettings,
        reader: Reader,
    ) -> QueryResult:
        expected_columns = [
            SelectedExpression(
                "org_id",
                Column("_snuba_org_id", None, "org_id"),
            ),
            SelectedExpression(
                "project_id",
                Column("_snuba_project_id", None, "project_id"),
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
            SelectedExpression(
                column_name,
                translated_value,
            ),
        ]

        assert clickhouse_query.get_selected_columns() == expected_columns
        return QueryResult(
            result={"meta": [], "data": [], "totals": {}},
            extra={"stats": {}, "sql": "", "experiments": {}},
        )

    entity = get_entity(entity_key)
    entity.get_query_pipeline_builder().build_execution_pipeline(
        request, query_runner
    ).execute()
