from datetime import datetime

import pytest

from snuba.clickhouse.query import Query as ClickhouseQuery
from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.query import OrderBy, OrderByDirection, SelectedExpression
from snuba.query.data_source.simple import Entity as QueryEntity
from snuba.query.dsl import Functions as f
from snuba.query.dsl import NestedColumn, and_cond, column, in_cond, literal
from snuba.query.dsl_mapper import DSLMapperVisitor, query_repr
from snuba.query.expressions import (
    Column,
    Expression,
    FunctionCall,
    Literal,
    SubscriptableReference,
)
from snuba.query.logical import Query as LogicalQuery

tags_raw = NestedColumn("tags_raw")

tests = [
    pytest.param(
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
                        Literal(None, 3600),
                    ),
                ),
                FunctionCall(
                    None,
                    "in",
                    (
                        Column("_snuba_project_id", None, "project_id"),
                        FunctionCall(
                            None,
                            "tuple",
                            (Literal(None, 1),),
                        ),
                    ),
                ),
            ),
        ),
        """and_cond(f.equals(column('granularity', None, '_snuba_granularity'), literal(3600)), in_cond(column('project_id', None, '_snuba_project_id'), f.tuple(literal(1))))""",
    ),
    pytest.param(
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
                                                    "and",
                                                    (
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
                                                        FunctionCall(
                                                            None,
                                                            "and",
                                                            (
                                                                FunctionCall(
                                                                    None,
                                                                    "equals",
                                                                    (
                                                                        Column(
                                                                            "_snuba_metric_id",
                                                                            None,
                                                                            "metric_id",
                                                                        ),
                                                                        Literal(
                                                                            None,
                                                                            123456,
                                                                        ),
                                                                    ),
                                                                ),
                                                                FunctionCall(
                                                                    None,
                                                                    "in",
                                                                    (
                                                                        SubscriptableReference(
                                                                            "_snuba_tags_raw[888]",
                                                                            column=Column(
                                                                                "_snuba_tags_raw",
                                                                                None,
                                                                                "tags_raw",
                                                                            ),
                                                                            key=Literal(
                                                                                None,
                                                                                "888",
                                                                            ),
                                                                        ),
                                                                        FunctionCall(
                                                                            None,
                                                                            "tuple",
                                                                            (
                                                                                Literal(
                                                                                    None,
                                                                                    "dist1",
                                                                                ),
                                                                                Literal(
                                                                                    None,
                                                                                    "dist2",
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
                        ),
                    ),
                ),
            ),
        ),
        """and_cond(f.equals(column('granularity', None, '_snuba_granularity'), literal(60)), and_cond(in_cond(column('project_id', None, '_snuba_project_id'), f.tuple(literal(11))), and_cond(in_cond(column('org_id', None, '_snuba_org_id'), f.tuple(literal(1))), and_cond(f.equals(column('use_case_id', None, '_snuba_use_case_id'), literal('transactions')), and_cond(f.greaterOrEquals(column('timestamp', None, '_snuba_timestamp'), literal(datetime(2023, 11, 23, 18, 30))), and_cond(f.less(column('timestamp', None, '_snuba_timestamp'), literal(datetime(2023, 11, 23, 22, 30))), and_cond(f.equals(column('metric_id', None, '_snuba_metric_id'), literal(123456)), in_cond(tags_raw['888'], f.tuple(literal('dist1'), literal('dist2'))))))))))""",
    ),
    pytest.param(
        and_cond(
            and_cond(
                and_cond(
                    f.greaterOrEquals(
                        column("timestamp", None, "_snuba_timestamp"),
                        literal(datetime(2023, 11, 23, 18, 30)),
                    ),
                    f.less(
                        column("timestamp", None, "_snuba_timestamp"),
                        literal(datetime(2023, 11, 23, 22, 30)),
                    ),
                ),
                and_cond(
                    in_cond(
                        column("project_id", None, "_snuba_project_id"),
                        f.tuple(literal(11)),
                    ),
                    in_cond(
                        column("org_id", None, "_snuba_org_id"), f.tuple(literal(1))
                    ),
                ),
            ),
            and_cond(
                and_cond(
                    f.equals(
                        column("use_case_id", None, "_snuba_use_case_id"),
                        literal("transactions"),
                    ),
                    f.equals(
                        column("granularity", None, "_snuba_granularity"), literal(60)
                    ),
                ),
                and_cond(
                    f.equals(
                        column("metric_id", None, "_snuba_metric_id"), literal(123456)
                    ),
                    in_cond(
                        tags_raw["888"], f.tuple(literal("dist1"), literal("dist2"))
                    ),
                ),
            ),
        ),
        """and_cond(and_cond(and_cond(f.greaterOrEquals(column('timestamp', None, '_snuba_timestamp'), literal(datetime(2023, 11, 23, 18, 30))), f.less(column('timestamp', None, '_snuba_timestamp'), literal(datetime(2023, 11, 23, 22, 30)))), and_cond(in_cond(column('project_id', None, '_snuba_project_id'), f.tuple(literal(11))), in_cond(column('org_id', None, '_snuba_org_id'), f.tuple(literal(1))))), and_cond(and_cond(f.equals(column('use_case_id', None, '_snuba_use_case_id'), literal('transactions')), f.equals(column('granularity', None, '_snuba_granularity'), literal(60))), and_cond(f.equals(column('metric_id', None, '_snuba_metric_id'), literal(123456)), in_cond(tags_raw['888'], f.tuple(literal('dist1'), literal('dist2'))))))""",
    ),
]


@pytest.mark.parametrize("condition, expected", tests)
def test_ast_repr(condition: Expression, expected: str) -> None:
    actual = condition.accept(DSLMapperVisitor())
    assert actual == expected


query_tests = [
    pytest.param(
        LogicalQuery(
            QueryEntity(
                EntityKey.GENERIC_METRICS_DISTRIBUTIONS,
                get_entity(EntityKey.GENERIC_METRICS_DISTRIBUTIONS).get_data_model(),
            ),
            selected_columns=[
                SelectedExpression(
                    "aggregate_value",
                    FunctionCall(
                        "_snuba_aggregate_value",
                        "sum",
                        (Column("_snuba_value", None, "value"),),
                    ),
                ),
            ],
            groupby=[],
            condition=FunctionCall(
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
                                                                    2021, 1, 1, 0, 0
                                                                ),
                                                            ),
                                                        ),
                                                    ),
                                                    FunctionCall(
                                                        None,
                                                        "and",
                                                        (
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
                                                                            2021,
                                                                            1,
                                                                            2,
                                                                            0,
                                                                            0,
                                                                        ),
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
                                                                                "_snuba_metric_id",
                                                                                None,
                                                                                "metric_id",
                                                                            ),
                                                                            Literal(
                                                                                None,
                                                                                123456,
                                                                            ),
                                                                        ),
                                                                    ),
                                                                    FunctionCall(
                                                                        None,
                                                                        "in",
                                                                        (
                                                                            SubscriptableReference(
                                                                                "_snuba_tags_raw[888]",
                                                                                column=Column(
                                                                                    "_snuba_tags_raw",
                                                                                    None,
                                                                                    "tags_raw",
                                                                                ),
                                                                                key=Literal(
                                                                                    None,
                                                                                    "888",
                                                                                ),
                                                                            ),
                                                                            FunctionCall(
                                                                                None,
                                                                                "tuple",
                                                                                (
                                                                                    Literal(
                                                                                        None,
                                                                                        "dist1",
                                                                                    ),
                                                                                    Literal(
                                                                                        None,
                                                                                        "dist2",
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
                            ),
                        ),
                    ),
                ),
            ),
            order_by=[
                OrderBy(
                    OrderByDirection.ASC,
                    FunctionCall(
                        alias="_snuba_aggregate_value",
                        function_name="sum",
                        parameters=(
                            (
                                Column(
                                    alias="_snuba_value",
                                    table_name=None,
                                    column_name="value",
                                ),
                            )
                        ),
                    ),
                ),
            ],
            limit=1000,
        ),
        """Query(
        from_clause=Entity(EntityKey.GENERIC_METRICS_DISTRIBUTIONS,get_entity(EntityKey.GENERIC_METRICS_DISTRIBUTIONS).get_data_model()),
        selected_columns=[SelectedExpression('aggregate_value', f.sum(column('value', None, '_snuba_value'), alias='_snuba_aggregate_value'))],
        array_join=None,
        condition=and_cond(f.equals(column('granularity', None, '_snuba_granularity'), literal(60)), and_cond(in_cond(column('project_id', None, '_snuba_project_id'), f.tuple(literal(1))), and_cond(in_cond(column('org_id', None, '_snuba_org_id'), f.tuple(literal(1))), and_cond(f.equals(column('use_case_id', None, '_snuba_use_case_id'), literal('transactions')), and_cond(f.greaterOrEquals(column('timestamp', None, '_snuba_timestamp'), literal(datetime(2021, 1, 1, 0, 0))), and_cond(f.less(column('timestamp', None, '_snuba_timestamp'), literal(datetime(2021, 1, 2, 0, 0))), and_cond(f.equals(column('metric_id', None, '_snuba_metric_id'), literal(123456)), in_cond(tags_raw['888'], f.tuple(literal('dist1'), literal('dist2')))))))))),
        groupby=None,
        having=None,
        order_by=[OrderBy(OrderByDirection.ASC, f.sum(column('value', None, '_snuba_value'), alias='_snuba_aggregate_value'))],
        limitby=None,
        limit=1000,
        offset=0,
        totals=False,
        granularity=None,
    )""",
    )
]


@pytest.mark.parametrize("query, expected", query_tests)
def test_dsl_query(query: LogicalQuery | ClickhouseQuery, expected: str) -> None:
    assert query_repr(query) == expected
