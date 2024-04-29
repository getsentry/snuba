"""
These are a small number of representative tests for the pipeline as a whole.
These verify that all the components of the pipeline are working together as expected.
It will include: general case snql, mql, formula mql.
"""


from datetime import datetime

from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.factory import get_dataset
from snuba.query import OrderBy, OrderByDirection, SelectedExpression
from snuba.query.conditions import binary_condition
from snuba.query.data_source.simple import Entity as QueryEntity
from snuba.query.dsl import and_cond, divide, equals, or_cond
from snuba.query.expressions import Column, FunctionCall, Literal
from snuba.query.logical import Query
from snuba.query.mql.parser import parse_mql_query
from tests.query.parser.test_formula_mql_query import tag_column, timeseries

from_distributions = QueryEntity(
    EntityKey.GENERIC_METRICS_DISTRIBUTIONS,
    get_entity(EntityKey.GENERIC_METRICS_DISTRIBUTIONS).get_data_model(),
)

time_expression = FunctionCall(
    "_snuba_time",
    "toStartOfInterval",
    (
        Column("_snuba_timestamp", None, "timestamp"),
        FunctionCall(None, "toIntervalSecond", (Literal(None, 60),)),
        Literal(None, "Universal"),
    ),
)


def test_formula() -> None:
    query_body = "sum(`d:transactions/duration@millisecond`){status_code:200} / sum(`d:transactions/duration@millisecond`)"
    exp_filter_in_select_condition = or_cond(
        and_cond(
            equals(
                tag_column("status_code"),
                Literal(None, "200"),
            ),
            equals(
                Column("_snuba_metric_id", None, "metric_id"),
                Literal(None, 123456),
            ),
        ),
        equals(
            Column("_snuba_metric_id", None, "metric_id"),
            Literal(None, 123456),
        ),
    )
    exp_condition = binary_condition(
        "and",
        exp_filter_in_select_condition,
        FunctionCall(
            None,
            "and",
            (
                FunctionCall(
                    None,
                    "equals",
                    (
                        Column(
                            "_snuba_granularity",
                            None,
                            "granularity",
                        ),
                        Literal(None, 60),
                    ),
                ),
                FunctionCall(
                    None,
                    "and",
                    (
                        FunctionCall(
                            None,
                            "in",
                            (
                                Column(
                                    "_snuba_project_id",
                                    None,
                                    "project_id",
                                ),
                                FunctionCall(
                                    None,
                                    "tuple",
                                    (Literal(None, 11),),
                                ),
                            ),
                        ),
                        FunctionCall(
                            None,
                            "and",
                            (
                                FunctionCall(
                                    None,
                                    "in",
                                    (
                                        Column(
                                            "_snuba_org_id",
                                            None,
                                            "org_id",
                                        ),
                                        FunctionCall(
                                            None,
                                            "tuple",
                                            (Literal(None, 1),),
                                        ),
                                    ),
                                ),
                                FunctionCall(
                                    None,
                                    "and",
                                    (
                                        FunctionCall(
                                            None,
                                            "equals",
                                            (
                                                Column(
                                                    "_snuba_use_case_id",
                                                    None,
                                                    "use_case_id",
                                                ),
                                                Literal(None, "transactions"),
                                            ),
                                        ),
                                        FunctionCall(
                                            None,
                                            "and",
                                            (
                                                FunctionCall(
                                                    None,
                                                    "greaterOrEquals",
                                                    (
                                                        Column(
                                                            "_snuba_timestamp",
                                                            None,
                                                            "timestamp",
                                                        ),
                                                        Literal(
                                                            None,
                                                            datetime(
                                                                2023, 11, 23, 18, 30
                                                            ),
                                                        ),
                                                    ),
                                                ),
                                                FunctionCall(
                                                    None,
                                                    "less",
                                                    (
                                                        Column(
                                                            "_snuba_timestamp",
                                                            None,
                                                            "timestamp",
                                                        ),
                                                        Literal(
                                                            None,
                                                            datetime(
                                                                2023,
                                                                11,
                                                                23,
                                                                22,
                                                                30,
                                                            ),
                                                        ),
                                                    ),
                                                ),
                                            ),
                                        ),
                                    ),
                                ),
                            ),
                        ),
                    ),
                ),
            ),
        ),
    )
    exp_selected = [
        SelectedExpression(
            "aggregate_value",
            divide(
                timeseries(
                    "sumIf",
                    123456,
                    binary_condition(
                        "equals", tag_column("status_code"), Literal(None, "200")
                    ),
                ),
                timeseries("sumIf", 123456),
                "_snuba_aggregate_value",
            ),
        ),
        SelectedExpression(
            "time",
            time_expression,
        ),
    ]
    expected = Query(
        from_distributions,
        selected_columns=exp_selected,
        groupby=[time_expression],
        condition=exp_condition,
        order_by=[
            OrderBy(
                direction=OrderByDirection.ASC,
                expression=time_expression,
            )
        ],
        limit=1000,
        offset=0,
    )
    generic_metrics = get_dataset(
        "generic_metrics",
    )
    mql_context = {
        "entity": "generic_metrics_distributions",
        "start": "2023-11-23T18:30:00",
        "end": "2023-11-23T22:30:00",
        "rollup": {
            "granularity": 60,
            "interval": 60,
            "with_totals": "False",
            "orderby": None,
        },
        "scope": {
            "org_ids": [1],
            "project_ids": [11],
            "use_case_id": "transactions",
        },
        "indexer_mappings": {
            "d:transactions/duration@millisecond": 123456,
            "status_code": 222222,
            "transaction": 333333,
        },
        "limit": None,
        "offset": None,
    }
    query, _ = parse_mql_query(str(query_body), mql_context, generic_metrics)
    eq, reason = query.equals(expected)
    assert eq, reason
