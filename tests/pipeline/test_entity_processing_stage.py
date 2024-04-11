from datetime import datetime

from snuba.attribution import get_app_id
from snuba.attribution.attribution_info import AttributionInfo
from snuba.clickhouse.query import Query
from snuba.datasets.factory import get_dataset
from snuba.datasets.storages.storage_key import StorageKey
from snuba.pipeline.query_pipeline import QueryPipelineResult
from snuba.pipeline.stages.query_processing import EntityProcessingStage
from snuba.query import SelectedExpression
from snuba.query.dsl import and_cond, binary_condition, column, equals, literal
from snuba.query.expressions import Column, FunctionCall
from snuba.query.query_settings import HTTPQuerySettings
from snuba.query.snql.parser import parse_snql_query
from snuba.request import Request
from snuba.utils.metrics.timer import Timer


def test_basic():
    query_body = {
        "query": (
            "MATCH (metrics_distributions) "
            "SELECT avg(granularity) "
            "WHERE "
            "timestamp >= toDateTime('2021-05-17 19:42:01') AND "
            "timestamp < toDateTime('2021-05-17 23:42:01') AND "
            "org_id = 1 AND "
            "project_id = 1"
        ),
    }
    metrics_dataset = get_dataset("metrics")
    query, _ = parse_snql_query(query_body["query"], metrics_dataset)
    query_settings = HTTPQuerySettings()
    timer = Timer("test")
    request = request = Request(
        id="",
        original_body=query_body,
        query=query,
        snql_anonymized="",
        query_settings=query_settings,
        attribution_info=AttributionInfo(
            get_app_id("blah"), {"tenant_type": "tenant_id"}, "blah", None, None, None
        ),
    )
    res = EntityProcessingStage().execute(
        QueryPipelineResult(
            data=request,
            query_settings=request.query_settings,
            timer=timer,
            error=None,
        )
    )
    assert res.data and not res.error
    assert res.query_settings == query_settings and res.timer == timer
    assert isinstance(res.data, Query)
    actual = res.data
    assert (
        actual.get_arrayjoin() is None
        and actual.get_prewhere_ast() is None
        and actual.get_groupby() == []
        and actual.get_having() is None
        and actual.get_orderby() == []
        and actual.get_limitby() is None
        and actual.get_offset() == 0
        and not actual.has_totals()
        and actual.get_granularity() is None
        and actual.get_limit() == 1000
    )
    assert actual.get_from_clause().storage_key == StorageKey.METRICS_DISTRIBUTIONS
    expected_selected = [
        SelectedExpression(
            "avg(granularity)",
            FunctionCall(
                "_snuba_avg(granularity)",
                "avg",
                (Column("_snuba_granularity", None, "granularity"),),
            ),
        )
    ]
    assert actual.get_selected_columns() == expected_selected
    expected_cond = and_cond(
        equals(column("granularity"), literal(60)),
        and_cond(
            binary_condition(
                "greaterOrEquals",
                column("timestamp", alias="_snuba_timestamp"),
                literal(datetime(2021, 5, 17, 19, 42, 1)),
            ),
            and_cond(
                binary_condition(
                    "less",
                    column("timestamp", alias="_snuba_timestamp"),
                    literal(datetime(2021, 5, 17, 23, 42, 1)),
                ),
                and_cond(
                    equals(column("org_id", alias="_snuba_org_id"), literal(1)),
                    equals(column("project_id", alias="_snuba_project_id"), literal(1)),
                ),
            ),
        ),
    )
    assert actual.get_condition() == expected_cond
