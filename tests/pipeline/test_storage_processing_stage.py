from copy import deepcopy
from datetime import datetime

from snuba.clickhouse.columns import ColumnSet
from snuba.clickhouse.query import Query
from snuba.datasets.storages.storage_key import StorageKey
from snuba.pipeline.query_pipeline import QueryPipelineResult
from snuba.pipeline.stages.query_processing import StorageProcessingStage
from snuba.query import SelectedExpression
from snuba.query.data_source.simple import Table
from snuba.query.dsl import and_cond, binary_condition, column, equals, literal
from snuba.query.expressions import Column, FunctionCall
from snuba.query.query_settings import HTTPQuerySettings
from snuba.utils.metrics.timer import Timer


def test_basic():
    ch_query = Query(
        from_clause=Table(
            "metrics_distributions_v2_local",
            ColumnSet([]),
            storage_key=StorageKey.METRICS_DISTRIBUTIONS,
        ),
        selected_columns=[
            SelectedExpression(
                "avg(granularity)",
                FunctionCall(
                    "_snuba_avg(granularity)",
                    "avg",
                    (Column("_snuba_granularity", None, "granularity"),),
                ),
            )
        ],
        condition=and_cond(
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
                        equals(
                            column("project_id", alias="_snuba_project_id"), literal(1)
                        ),
                    ),
                ),
            ),
        ),
        limit=1000,
    )
    timer = Timer("test")
    settings = HTTPQuerySettings()

    res = StorageProcessingStage().execute(
        QueryPipelineResult(
            data=deepcopy(ch_query),
            query_settings=settings,
            timer=timer,
            error=None,
        )
    )
    assert res.data == Query(
        from_clause=res.data.get_from_clause(),
        selected_columns=ch_query.get_selected_columns(),
        array_join=ch_query.get_arrayjoin(),
        condition=ch_query.get_condition(),
        prewhere=ch_query.get_prewhere_ast(),
        groupby=ch_query.get_groupby(),
        having=ch_query.get_having(),
        order_by=ch_query.get_orderby(),
        limitby=ch_query.get_limitby(),
        limit=ch_query.get_limit(),
        offset=ch_query.get_offset(),
        totals=ch_query.has_totals(),
        granularity=ch_query.get_granularity(),
    )
    assert res.data.get_from_clause() != ch_query.get_from_clause()
    assert res.data.get_from_clause().table_name == "metrics_distributions_v2_local"
    assert res.data.get_from_clause().storage_key == StorageKey.METRICS_DISTRIBUTIONS
    assert len(res.data.get_from_clause().schema.columns) > 0
