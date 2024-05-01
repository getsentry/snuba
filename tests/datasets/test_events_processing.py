import pytest

from snuba.attribution import get_app_id
from snuba.attribution.attribution_info import AttributionInfo
from snuba.datasets.factory import get_dataset
from snuba.pipeline.query_pipeline import QueryPipelineResult
from snuba.pipeline.stages.query_processing import (
    EntityProcessingStage,
    StorageProcessingStage,
)
from snuba.query import SelectedExpression
from snuba.query.expressions import Column, FunctionCall, Literal
from snuba.query.query_settings import HTTPQuerySettings
from snuba.query.snql.parser import parse_snql_query
from snuba.request import Request
from snuba.utils.metrics.timer import Timer


@pytest.mark.clickhouse_db
def test_events_processing() -> None:
    query_body = {
        "query": """
        MATCH (events)
        SELECT tags[transaction], contexts[browser.name]
        WHERE project_id = 1
        AND timestamp >= toDateTime('2020-01-01 12:00:00')
        AND timestamp < toDateTime('2020-01-02 12:00:00')
        """,
        "dataset": "events",
    }

    events_dataset = get_dataset("events")

    query = parse_snql_query(query_body["query"], events_dataset)
    request = Request(
        id="",
        original_body=query_body,
        query=query,
        query_settings=HTTPQuerySettings(referrer=""),
        attribution_info=AttributionInfo(
            get_app_id("blah"), {"tenant_type": "tenant_id"}, "blah", None, None, None
        ),
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

    assert clickhouse_query.get_selected_columns() == [
        SelectedExpression(
            "tags[transaction]",
            Column("_snuba_tags[transaction]", None, "transaction_name"),
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
