"""
These tests were auto-generated, many of them may be unnecessary or redundant, feel free to remove some.
This tests the first stage of the MQL parsing pipeline, which looks like MQL->AST.
It includes parsing of regular and formula mql.
"""

from datetime import datetime
from typing import Any

import pytest

from snuba.datasets.dataset import Dataset
from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.factory import get_dataset
from snuba.pipeline.query_pipeline import QueryPipelineResult
from snuba.query import OrderBy, OrderByDirection, SelectedExpression
from snuba.query.composite import CompositeQuery
from snuba.query.data_source.join import (
    IndividualNode,
    JoinClause,
    JoinCondition,
    JoinConditionExpression,
    JoinType,
)
from snuba.query.data_source.simple import Entity
from snuba.query.dsl import Functions as f
from snuba.query.dsl import and_cond, column, divide, in_cond, literal, multiply, plus
from snuba.query.expressions import (
    Column,
    CurriedFunctionCall,
    FunctionCall,
    Literal,
    SubscriptableReference,
)
from snuba.query.logical import Query
from snuba.query.mql.parser import ParsePopulateResolveMQL
from snuba.query.parser.exceptions import ParsingException
from snuba.query.query_settings import HTTPQuerySettings, QuerySettings
from snuba.utils.metrics.timer import Timer


def subscriptable_expression(
    tag_key: str, table_alias: str | None = None
) -> SubscriptableReference:
    return SubscriptableReference(
        alias=None,
        column=Column(alias=None, table_name=table_alias, column_name="tags_raw"),
        key=Literal(alias=None, value=tag_key),
    )


from_distributions = Entity(
    EntityKey.GENERIC_METRICS_DISTRIBUTIONS,
    get_entity(EntityKey.GENERIC_METRICS_DISTRIBUTIONS).get_data_model(),
)
join_clause = JoinClause(
    left_node=IndividualNode(
        alias="d1",
        data_source=from_distributions,
    ),
    right_node=IndividualNode(
        alias="d0",
        data_source=from_distributions,
    ),
    keys=[
        JoinCondition(
            left=JoinConditionExpression(table_alias="d1", column="d1.time"),
            right=JoinConditionExpression(table_alias="d0", column="d0.time"),
        )
    ],
    join_type=JoinType.INNER,
    join_modifier=None,
)
join_clause_with_groupby = JoinClause(
    left_node=IndividualNode(
        alias="d1",
        data_source=from_distributions,
    ),
    right_node=IndividualNode(
        alias="d0",
        data_source=from_distributions,
    ),
    keys=[
        JoinCondition(
            left=JoinConditionExpression(table_alias="d1", column="tags_raw[333333]"),
            right=JoinConditionExpression(table_alias="d0", column="tags_raw[333333]"),
        ),
        JoinCondition(
            left=JoinConditionExpression(table_alias="d1", column="d1.time"),
            right=JoinConditionExpression(table_alias="d0", column="d0.time"),
        ),
    ],
    join_type=JoinType.INNER,
    join_modifier=None,
)


def time_expression(
    table_alias: str | None = None, to_interval_seconds: int = 60
) -> FunctionCall:
    alias_prefix = f"{table_alias}." if table_alias else ""
    return FunctionCall(
        f"{alias_prefix}time",
        "toStartOfInterval",
        (
            Column(None, table_alias, "timestamp"),
            FunctionCall(
                None, "toIntervalSecond", (Literal(None, to_interval_seconds),)
            ),
            Literal(None, "Universal"),
        ),
    )


test_cases = [
    pytest.param(
        (
            "sum(`d:transactions/duration@millisecond`){status_code:200} / sum(`d:transactions/duration@millisecond`)",
            get_dataset("generic_metrics"),
            {
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
            },
            None,
        ),
        CompositeQuery(
            from_clause=join_clause,
            selected_columns=[
                SelectedExpression(
                    "aggregate_value",
                    divide(
                        FunctionCall(
                            None,
                            "sum",
                            (Column(None, "d0", "value"),),
                        ),
                        FunctionCall(
                            None,
                            "sum",
                            (Column(None, "d1", "value"),),
                        ),
                        "aggregate_value",
                    ),
                ),
                SelectedExpression(
                    "time",
                    time_expression("d1"),
                ),
                SelectedExpression(
                    "time",
                    time_expression("d0"),
                ),
            ],
            array_join=None,
            condition=and_cond(
                and_cond(
                    and_cond(
                        f.greaterOrEquals(
                            column("timestamp", "d0"),
                            literal(datetime(2023, 11, 23, 18, 30)),
                        ),
                        f.less(
                            column("timestamp", "d0"),
                            literal(datetime(2023, 11, 23, 22, 30)),
                        ),
                    ),
                    and_cond(
                        and_cond(
                            in_cond(column("project_id", "d0"), f.tuple(literal(11))),
                            and_cond(
                                in_cond(column("org_id", "d0"), f.tuple(literal(1))),
                                f.equals(
                                    column("use_case_id", "d0"), literal("transactions")
                                ),
                            ),
                        ),
                        f.equals(column("granularity", "d0"), literal(60)),
                    ),
                ),
                and_cond(
                    and_cond(
                        and_cond(
                            f.greaterOrEquals(
                                column("timestamp", "d1"),
                                literal(datetime(2023, 11, 23, 18, 30)),
                            ),
                            f.less(
                                column("timestamp", "d1"),
                                literal(datetime(2023, 11, 23, 22, 30)),
                            ),
                        ),
                        and_cond(
                            and_cond(
                                in_cond(
                                    column("project_id", "d1"), f.tuple(literal(11))
                                ),
                                and_cond(
                                    in_cond(
                                        column("org_id", "d1"), f.tuple(literal(1))
                                    ),
                                    f.equals(
                                        column("use_case_id", "d1"),
                                        literal("transactions"),
                                    ),
                                ),
                            ),
                            f.equals(column("granularity", "d1"), literal(60)),
                        ),
                    ),
                    and_cond(
                        f.equals(column("tags_raw[222222]", "d0"), literal("200")),
                        and_cond(
                            f.equals(column("metric_id", "d0"), literal(123456)),
                            f.equals(column("metric_id", "d1"), literal(123456)),
                        ),
                    ),
                ),
            ),
            groupby=[time_expression("d1"), time_expression("d0")],
            having=None,
            order_by=[OrderBy(OrderByDirection.ASC, time_expression("d0"))],
            limitby=None,
            limit=1000,
            offset=0,
            totals=False,
            granularity=None,
        ),
    ),
    pytest.param(
        (
            "1 + sum(`d:transactions/duration@millisecond`){status_code:200} / sum(`d:transactions/duration@millisecond`)",
            get_dataset("generic_metrics"),
            {
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
            },
            None,
        ),
        CompositeQuery(
            from_clause=join_clause,
            selected_columns=[
                SelectedExpression(
                    "aggregate_value",
                    plus(
                        Literal(None, 1.0),
                        divide(
                            FunctionCall(
                                None,
                                "sum",
                                (Column(None, "d0", "value"),),
                            ),
                            FunctionCall(
                                None,
                                "sum",
                                (Column(None, "d1", "value"),),
                            ),
                        ),
                        "aggregate_value",
                    ),
                ),
                SelectedExpression(
                    "time",
                    time_expression("d1"),
                ),
                SelectedExpression(
                    "time",
                    time_expression("d0"),
                ),
            ],
            array_join=None,
            condition=and_cond(
                and_cond(
                    and_cond(
                        f.greaterOrEquals(
                            column("timestamp", "d0"),
                            literal(datetime(2023, 11, 23, 18, 30)),
                        ),
                        f.less(
                            column("timestamp", "d0"),
                            literal(datetime(2023, 11, 23, 22, 30)),
                        ),
                    ),
                    and_cond(
                        and_cond(
                            in_cond(column("project_id", "d0"), f.tuple(literal(11))),
                            and_cond(
                                in_cond(column("org_id", "d0"), f.tuple(literal(1))),
                                f.equals(
                                    column("use_case_id", "d0"), literal("transactions")
                                ),
                            ),
                        ),
                        f.equals(column("granularity", "d0"), literal(60)),
                    ),
                ),
                and_cond(
                    and_cond(
                        and_cond(
                            f.greaterOrEquals(
                                column("timestamp", "d1"),
                                literal(datetime(2023, 11, 23, 18, 30)),
                            ),
                            f.less(
                                column("timestamp", "d1"),
                                literal(datetime(2023, 11, 23, 22, 30)),
                            ),
                        ),
                        and_cond(
                            and_cond(
                                in_cond(
                                    column("project_id", "d1"), f.tuple(literal(11))
                                ),
                                and_cond(
                                    in_cond(
                                        column("org_id", "d1"), f.tuple(literal(1))
                                    ),
                                    f.equals(
                                        column("use_case_id", "d1"),
                                        literal("transactions"),
                                    ),
                                ),
                            ),
                            f.equals(column("granularity", "d1"), literal(60)),
                        ),
                    ),
                    and_cond(
                        f.equals(column("tags_raw[222222]", "d0"), literal("200")),
                        and_cond(
                            f.equals(column("metric_id", "d0"), literal(123456)),
                            f.equals(column("metric_id", "d1"), literal(123456)),
                        ),
                    ),
                ),
            ),
            groupby=[time_expression("d1"), time_expression("d0")],
            having=None,
            order_by=[OrderBy(OrderByDirection.ASC, time_expression("d0"))],
            limitby=None,
            limit=1000,
            offset=0,
            totals=False,
            granularity=None,
        ),
    ),
    pytest.param(
        (
            "sum(`d:transactions/duration@millisecond`){status_code:200} by transaction / sum(`d:transactions/duration@millisecond`) by transaction",
            get_dataset("generic_metrics"),
            {
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
            },
            None,
        ),
        CompositeQuery(
            from_clause=join_clause_with_groupby,
            selected_columns=[
                SelectedExpression(
                    "aggregate_value",
                    divide(
                        FunctionCall(
                            None,
                            "sum",
                            (Column(None, "d0", "value"),),
                        ),
                        FunctionCall(
                            None,
                            "sum",
                            (Column(None, "d1", "value"),),
                        ),
                        "aggregate_value",
                    ),
                ),
                SelectedExpression(
                    "transaction",
                    column("tags_raw[333333]", "d0", "d0.transaction"),
                ),
                SelectedExpression(
                    "transaction",
                    column("tags_raw[333333]", "d1", "d1.transaction"),
                ),
                SelectedExpression(
                    "time",
                    time_expression("d1"),
                ),
                SelectedExpression(
                    "time",
                    time_expression("d0"),
                ),
            ],
            array_join=None,
            condition=and_cond(
                and_cond(
                    and_cond(
                        f.greaterOrEquals(
                            column("timestamp", "d0"),
                            literal(datetime(2023, 11, 23, 18, 30)),
                        ),
                        f.less(
                            column("timestamp", "d0"),
                            literal(datetime(2023, 11, 23, 22, 30)),
                        ),
                    ),
                    and_cond(
                        and_cond(
                            in_cond(column("project_id", "d0"), f.tuple(literal(11))),
                            and_cond(
                                in_cond(column("org_id", "d0"), f.tuple(literal(1))),
                                f.equals(
                                    column("use_case_id", "d0"), literal("transactions")
                                ),
                            ),
                        ),
                        f.equals(column("granularity", "d0"), literal(60)),
                    ),
                ),
                and_cond(
                    and_cond(
                        and_cond(
                            f.greaterOrEquals(
                                column("timestamp", "d1"),
                                literal(datetime(2023, 11, 23, 18, 30)),
                            ),
                            f.less(
                                column("timestamp", "d1"),
                                literal(datetime(2023, 11, 23, 22, 30)),
                            ),
                        ),
                        and_cond(
                            and_cond(
                                in_cond(
                                    column("project_id", "d1"), f.tuple(literal(11))
                                ),
                                and_cond(
                                    in_cond(
                                        column("org_id", "d1"), f.tuple(literal(1))
                                    ),
                                    f.equals(
                                        column("use_case_id", "d1"),
                                        literal("transactions"),
                                    ),
                                ),
                            ),
                            f.equals(column("granularity", "d1"), literal(60)),
                        ),
                    ),
                    and_cond(
                        f.equals(column("tags_raw[222222]", "d0"), literal("200")),
                        and_cond(
                            f.equals(column("metric_id", "d0"), literal(123456)),
                            f.equals(column("metric_id", "d1"), literal(123456)),
                        ),
                    ),
                ),
            ),
            groupby=[
                column("tags_raw[333333]", "d0", "d0.transaction"),
                column("tags_raw[333333]", "d1", "d1.transaction"),
                time_expression("d1"),
                time_expression("d0"),
            ],
            having=None,
            order_by=[OrderBy(OrderByDirection.ASC, time_expression("d0"))],
            limitby=None,
            limit=1000,
            offset=0,
            totals=False,
            granularity=None,
        ),
    ),
    pytest.param(
        (
            "quantiles(0.5)(`d:transactions/duration@millisecond`){status_code:200} by transaction / sum(`d:transactions/duration@millisecond`) by transaction",
            get_dataset("generic_metrics"),
            {
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
            },
            None,
        ),
        CompositeQuery(
            from_clause=join_clause_with_groupby,
            selected_columns=[
                SelectedExpression(
                    "aggregate_value",
                    divide(
                        CurriedFunctionCall(
                            None,
                            f.quantiles(literal(0.5)),
                            (column("value", "d0"),),
                        ),
                        FunctionCall(
                            None,
                            "sum",
                            (Column(None, "d1", "value"),),
                        ),
                        "aggregate_value",
                    ),
                ),
                SelectedExpression(
                    "transaction",
                    column("tags_raw[333333]", "d0", "d0.transaction"),
                ),
                SelectedExpression(
                    "transaction",
                    column("tags_raw[333333]", "d1", "d1.transaction"),
                ),
                SelectedExpression(
                    "time",
                    time_expression("d1"),
                ),
                SelectedExpression(
                    "time",
                    time_expression("d0"),
                ),
            ],
            array_join=None,
            condition=and_cond(
                and_cond(
                    and_cond(
                        f.greaterOrEquals(
                            column("timestamp", "d0"),
                            literal(datetime(2023, 11, 23, 18, 30)),
                        ),
                        f.less(
                            column("timestamp", "d0"),
                            literal(datetime(2023, 11, 23, 22, 30)),
                        ),
                    ),
                    and_cond(
                        and_cond(
                            in_cond(column("project_id", "d0"), f.tuple(literal(11))),
                            and_cond(
                                in_cond(column("org_id", "d0"), f.tuple(literal(1))),
                                f.equals(
                                    column("use_case_id", "d0"), literal("transactions")
                                ),
                            ),
                        ),
                        f.equals(column("granularity", "d0"), literal(60)),
                    ),
                ),
                and_cond(
                    and_cond(
                        and_cond(
                            f.greaterOrEquals(
                                column("timestamp", "d1"),
                                literal(datetime(2023, 11, 23, 18, 30)),
                            ),
                            f.less(
                                column("timestamp", "d1"),
                                literal(datetime(2023, 11, 23, 22, 30)),
                            ),
                        ),
                        and_cond(
                            and_cond(
                                in_cond(
                                    column("project_id", "d1"), f.tuple(literal(11))
                                ),
                                and_cond(
                                    in_cond(
                                        column("org_id", "d1"), f.tuple(literal(1))
                                    ),
                                    f.equals(
                                        column("use_case_id", "d1"),
                                        literal("transactions"),
                                    ),
                                ),
                            ),
                            f.equals(column("granularity", "d1"), literal(60)),
                        ),
                    ),
                    and_cond(
                        f.equals(column("tags_raw[222222]", "d0"), literal("200")),
                        and_cond(
                            f.equals(column("metric_id", "d0"), literal(123456)),
                            f.equals(column("metric_id", "d1"), literal(123456)),
                        ),
                    ),
                ),
            ),
            groupby=[
                column("tags_raw[333333]", "d0", "d0.transaction"),
                column("tags_raw[333333]", "d1", "d1.transaction"),
                time_expression("d1"),
                time_expression("d0"),
            ],
            having=None,
            order_by=[OrderBy(OrderByDirection.ASC, time_expression("d0"))],
            limitby=None,
            limit=1000,
            offset=0,
            totals=False,
            granularity=None,
        ),
    ),
    pytest.param(
        (
            "sum(`d:transactions/duration@millisecond`) / ((max(`d:transactions/duration@millisecond`) + avg(`d:transactions/duration@millisecond`)) * min(`d:transactions/duration@millisecond`))",
            get_dataset("generic_metrics"),
            {
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
            },
            None,
        ),
        CompositeQuery(
            from_clause=JoinClause(
                left_node=JoinClause(
                    left_node=JoinClause(
                        left_node=IndividualNode(
                            alias="d3",
                            data_source=from_distributions,
                        ),
                        right_node=IndividualNode(
                            alias="d2",
                            data_source=from_distributions,
                        ),
                        keys=[
                            JoinCondition(
                                left=JoinConditionExpression(
                                    table_alias="d3", column="d3.time"
                                ),
                                right=JoinConditionExpression(
                                    table_alias="d2", column="d2.time"
                                ),
                            )
                        ],
                        join_type=JoinType.INNER,
                        join_modifier=None,
                    ),
                    right_node=IndividualNode(
                        alias="d1",
                        data_source=from_distributions,
                    ),
                    keys=[
                        JoinCondition(
                            left=JoinConditionExpression(
                                table_alias="d2", column="d2.time"
                            ),
                            right=JoinConditionExpression(
                                table_alias="d1", column="d1.time"
                            ),
                        )
                    ],
                    join_type=JoinType.INNER,
                    join_modifier=None,
                ),
                right_node=IndividualNode(
                    alias="d0",
                    data_source=from_distributions,
                ),
                keys=[
                    JoinCondition(
                        left=JoinConditionExpression(
                            table_alias="d1", column="d1.time"
                        ),
                        right=JoinConditionExpression(
                            table_alias="d0", column="d0.time"
                        ),
                    )
                ],
                join_type=JoinType.INNER,
                join_modifier=None,
            ),
            selected_columns=[
                SelectedExpression(
                    "aggregate_value",
                    divide(
                        FunctionCall(
                            None,
                            "sum",
                            (Column(None, "d0", "value"),),
                        ),
                        multiply(
                            plus(
                                FunctionCall(
                                    None,
                                    "max",
                                    (Column(None, "d1", "value"),),
                                ),
                                FunctionCall(
                                    None,
                                    "avg",
                                    (Column(None, "d2", "value"),),
                                ),
                            ),
                            FunctionCall(
                                None,
                                "min",
                                (Column(None, "d3", "value"),),
                            ),
                        ),
                        "aggregate_value",
                    ),
                ),
                SelectedExpression(
                    "time",
                    time_expression("d3"),
                ),
                SelectedExpression(
                    "time",
                    time_expression("d2"),
                ),
                SelectedExpression(
                    "time",
                    time_expression("d1"),
                ),
                SelectedExpression(
                    "time",
                    time_expression("d0"),
                ),
            ],
            array_join=None,
            condition=and_cond(
                and_cond(
                    and_cond(
                        f.greaterOrEquals(
                            column("timestamp", "d0"),
                            literal(datetime(2023, 11, 23, 18, 30)),
                        ),
                        f.less(
                            column("timestamp", "d0"),
                            literal(datetime(2023, 11, 23, 22, 30)),
                        ),
                    ),
                    and_cond(
                        and_cond(
                            in_cond(column("project_id", "d0"), f.tuple(literal(11))),
                            and_cond(
                                in_cond(column("org_id", "d0"), f.tuple(literal(1))),
                                f.equals(
                                    column("use_case_id", "d0"), literal("transactions")
                                ),
                            ),
                        ),
                        f.equals(column("granularity", "d0"), literal(60)),
                    ),
                ),
                and_cond(
                    and_cond(
                        and_cond(
                            f.greaterOrEquals(
                                column("timestamp", "d1"),
                                literal(datetime(2023, 11, 23, 18, 30)),
                            ),
                            f.less(
                                column("timestamp", "d1"),
                                literal(datetime(2023, 11, 23, 22, 30)),
                            ),
                        ),
                        and_cond(
                            and_cond(
                                in_cond(
                                    column("project_id", "d1"), f.tuple(literal(11))
                                ),
                                and_cond(
                                    in_cond(
                                        column("org_id", "d1"), f.tuple(literal(1))
                                    ),
                                    f.equals(
                                        column("use_case_id", "d1"),
                                        literal("transactions"),
                                    ),
                                ),
                            ),
                            f.equals(column("granularity", "d1"), literal(60)),
                        ),
                    ),
                    and_cond(
                        and_cond(
                            and_cond(
                                f.greaterOrEquals(
                                    column("timestamp", "d2"),
                                    literal(datetime(2023, 11, 23, 18, 30)),
                                ),
                                f.less(
                                    column("timestamp", "d2"),
                                    literal(datetime(2023, 11, 23, 22, 30)),
                                ),
                            ),
                            and_cond(
                                and_cond(
                                    in_cond(
                                        column("project_id", "d2"), f.tuple(literal(11))
                                    ),
                                    and_cond(
                                        in_cond(
                                            column("org_id", "d2"), f.tuple(literal(1))
                                        ),
                                        f.equals(
                                            column("use_case_id", "d2"),
                                            literal("transactions"),
                                        ),
                                    ),
                                ),
                                f.equals(column("granularity", "d2"), literal(60)),
                            ),
                        ),
                        and_cond(
                            and_cond(
                                and_cond(
                                    f.greaterOrEquals(
                                        column("timestamp", "d3"),
                                        literal(datetime(2023, 11, 23, 18, 30)),
                                    ),
                                    f.less(
                                        column("timestamp", "d3"),
                                        literal(datetime(2023, 11, 23, 22, 30)),
                                    ),
                                ),
                                and_cond(
                                    and_cond(
                                        in_cond(
                                            column("project_id", "d3"),
                                            f.tuple(literal(11)),
                                        ),
                                        and_cond(
                                            in_cond(
                                                column("org_id", "d3"),
                                                f.tuple(literal(1)),
                                            ),
                                            f.equals(
                                                column("use_case_id", "d3"),
                                                literal("transactions"),
                                            ),
                                        ),
                                    ),
                                    f.equals(column("granularity", "d3"), literal(60)),
                                ),
                            ),
                            and_cond(
                                and_cond(
                                    f.equals(
                                        column("metric_id", "d0"), literal(123456)
                                    ),
                                    f.equals(
                                        column("metric_id", "d1"), literal(123456)
                                    ),
                                ),
                                and_cond(
                                    f.equals(
                                        column("metric_id", "d2"), literal(123456)
                                    ),
                                    f.equals(
                                        column("metric_id", "d3"), literal(123456)
                                    ),
                                ),
                            ),
                        ),
                    ),
                ),
            ),
            groupby=[
                time_expression("d3"),
                time_expression("d2"),
                time_expression("d1"),
                time_expression("d0"),
            ],
            having=None,
            order_by=[OrderBy(OrderByDirection.ASC, time_expression("d0"))],
            limitby=None,
            limit=1000,
            offset=0,
            totals=False,
            granularity=None,
        ),
    ),
    pytest.param(
        (
            "(sum(`d:transactions/duration@millisecond`) / max(`d:transactions/duration@millisecond`)){status_code:200}",
            get_dataset("generic_metrics"),
            {
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
            },
            None,
        ),
        CompositeQuery(
            from_clause=join_clause,
            selected_columns=[
                SelectedExpression(
                    "aggregate_value",
                    divide(
                        FunctionCall(
                            None,
                            "sum",
                            (Column(None, "d0", "value"),),
                        ),
                        FunctionCall(
                            None,
                            "max",
                            (Column(None, "d1", "value"),),
                        ),
                        "aggregate_value",
                    ),
                ),
                SelectedExpression(
                    "time",
                    time_expression("d1"),
                ),
                SelectedExpression(
                    "time",
                    time_expression("d0"),
                ),
            ],
            array_join=None,
            condition=and_cond(
                and_cond(
                    and_cond(
                        f.greaterOrEquals(
                            column("timestamp", "d0"),
                            literal(datetime(2023, 11, 23, 18, 30)),
                        ),
                        f.less(
                            column("timestamp", "d0"),
                            literal(datetime(2023, 11, 23, 22, 30)),
                        ),
                    ),
                    and_cond(
                        and_cond(
                            in_cond(column("project_id", "d0"), f.tuple(literal(11))),
                            and_cond(
                                in_cond(column("org_id", "d0"), f.tuple(literal(1))),
                                f.equals(
                                    column("use_case_id", "d0"), literal("transactions")
                                ),
                            ),
                        ),
                        f.equals(column("granularity", "d0"), literal(60)),
                    ),
                ),
                and_cond(
                    and_cond(
                        and_cond(
                            f.greaterOrEquals(
                                column("timestamp", "d1"),
                                literal(datetime(2023, 11, 23, 18, 30)),
                            ),
                            f.less(
                                column("timestamp", "d1"),
                                literal(datetime(2023, 11, 23, 22, 30)),
                            ),
                        ),
                        and_cond(
                            and_cond(
                                in_cond(
                                    column("project_id", "d1"), f.tuple(literal(11))
                                ),
                                and_cond(
                                    in_cond(
                                        column("org_id", "d1"), f.tuple(literal(1))
                                    ),
                                    f.equals(
                                        column("use_case_id", "d1"),
                                        literal("transactions"),
                                    ),
                                ),
                            ),
                            f.equals(column("granularity", "d1"), literal(60)),
                        ),
                    ),
                    and_cond(
                        and_cond(
                            f.equals(column("tags_raw[222222]", "d0"), literal("200")),
                            f.equals(column("metric_id", "d0"), literal(123456)),
                        ),
                        and_cond(
                            f.equals(column("tags_raw[222222]", "d1"), literal("200")),
                            f.equals(column("metric_id", "d1"), literal(123456)),
                        ),
                    ),
                ),
            ),
            groupby=[time_expression("d1"), time_expression("d0")],
            having=None,
            order_by=[OrderBy(OrderByDirection.ASC, time_expression("d0"))],
            limitby=None,
            limit=1000,
            offset=0,
            totals=False,
            granularity=None,
        ),
    ),
    pytest.param(
        (
            "(sum(`d:transactions/duration@millisecond`) / max(`d:transactions/duration@millisecond`)){status_code:200} by transaction",
            get_dataset("generic_metrics"),
            {
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
            },
            None,
        ),
        CompositeQuery(
            from_clause=join_clause_with_groupby,
            selected_columns=[
                SelectedExpression(
                    "aggregate_value",
                    divide(
                        FunctionCall(
                            None,
                            "sum",
                            (Column(None, "d0", "value"),),
                        ),
                        FunctionCall(
                            None,
                            "max",
                            (Column(None, "d1", "value"),),
                        ),
                        "aggregate_value",
                    ),
                ),
                SelectedExpression(
                    "transaction",
                    column("tags_raw[333333]", "d0", "d0.transaction"),
                ),
                SelectedExpression(
                    "transaction",
                    column("tags_raw[333333]", "d1", "d1.transaction"),
                ),
                SelectedExpression(
                    "time",
                    time_expression("d1"),
                ),
                SelectedExpression(
                    "time",
                    time_expression("d0"),
                ),
            ],
            array_join=None,
            condition=and_cond(
                and_cond(
                    and_cond(
                        f.greaterOrEquals(
                            column("timestamp", "d0"),
                            literal(datetime(2023, 11, 23, 18, 30)),
                        ),
                        f.less(
                            column("timestamp", "d0"),
                            literal(datetime(2023, 11, 23, 22, 30)),
                        ),
                    ),
                    and_cond(
                        and_cond(
                            in_cond(column("project_id", "d0"), f.tuple(literal(11))),
                            and_cond(
                                in_cond(column("org_id", "d0"), f.tuple(literal(1))),
                                f.equals(
                                    column("use_case_id", "d0"), literal("transactions")
                                ),
                            ),
                        ),
                        f.equals(column("granularity", "d0"), literal(60)),
                    ),
                ),
                and_cond(
                    and_cond(
                        and_cond(
                            f.greaterOrEquals(
                                column("timestamp", "d1"),
                                literal(datetime(2023, 11, 23, 18, 30)),
                            ),
                            f.less(
                                column("timestamp", "d1"),
                                literal(datetime(2023, 11, 23, 22, 30)),
                            ),
                        ),
                        and_cond(
                            and_cond(
                                in_cond(
                                    column("project_id", "d1"), f.tuple(literal(11))
                                ),
                                and_cond(
                                    in_cond(
                                        column("org_id", "d1"), f.tuple(literal(1))
                                    ),
                                    f.equals(
                                        column("use_case_id", "d1"),
                                        literal("transactions"),
                                    ),
                                ),
                            ),
                            f.equals(column("granularity", "d1"), literal(60)),
                        ),
                    ),
                    and_cond(
                        and_cond(
                            f.equals(column("tags_raw[222222]", "d0"), literal("200")),
                            f.equals(column("metric_id", "d0"), literal(123456)),
                        ),
                        and_cond(
                            f.equals(column("tags_raw[222222]", "d1"), literal("200")),
                            f.equals(column("metric_id", "d1"), literal(123456)),
                        ),
                    ),
                ),
            ),
            groupby=[
                column("tags_raw[333333]", "d0", "d0.transaction"),
                column("tags_raw[333333]", "d1", "d1.transaction"),
                time_expression("d1"),
                time_expression("d0"),
            ],
            having=None,
            order_by=[OrderBy(OrderByDirection.ASC, time_expression("d0"))],
            limitby=None,
            limit=1000,
            offset=0,
            totals=False,
            granularity=None,
        ),
    ),
    pytest.param(
        (
            "(sum(`d:transactions/duration@millisecond`) / sum(`d:transactions/duration@millisecond`)) + 100",
            get_dataset("generic_metrics"),
            {
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
            },
            None,
        ),
        CompositeQuery(
            from_clause=join_clause,
            selected_columns=[
                SelectedExpression(
                    "aggregate_value",
                    plus(
                        divide(
                            FunctionCall(
                                None,
                                "sum",
                                (Column(None, "d0", "value"),),
                            ),
                            FunctionCall(
                                None,
                                "sum",
                                (Column(None, "d1", "value"),),
                            ),
                        ),
                        Literal(None, 100.0),
                        "aggregate_value",
                    ),
                ),
                SelectedExpression(
                    "time",
                    time_expression("d1"),
                ),
                SelectedExpression(
                    "time",
                    time_expression("d0"),
                ),
            ],
            array_join=None,
            condition=and_cond(
                and_cond(
                    and_cond(
                        f.greaterOrEquals(
                            column("timestamp", "d0"),
                            literal(datetime(2023, 11, 23, 18, 30)),
                        ),
                        f.less(
                            column("timestamp", "d0"),
                            literal(datetime(2023, 11, 23, 22, 30)),
                        ),
                    ),
                    and_cond(
                        and_cond(
                            in_cond(column("project_id", "d0"), f.tuple(literal(11))),
                            and_cond(
                                in_cond(column("org_id", "d0"), f.tuple(literal(1))),
                                f.equals(
                                    column("use_case_id", "d0"), literal("transactions")
                                ),
                            ),
                        ),
                        f.equals(column("granularity", "d0"), literal(60)),
                    ),
                ),
                and_cond(
                    and_cond(
                        and_cond(
                            f.greaterOrEquals(
                                column("timestamp", "d1"),
                                literal(datetime(2023, 11, 23, 18, 30)),
                            ),
                            f.less(
                                column("timestamp", "d1"),
                                literal(datetime(2023, 11, 23, 22, 30)),
                            ),
                        ),
                        and_cond(
                            and_cond(
                                in_cond(
                                    column("project_id", "d1"), f.tuple(literal(11))
                                ),
                                and_cond(
                                    in_cond(
                                        column("org_id", "d1"), f.tuple(literal(1))
                                    ),
                                    f.equals(
                                        column("use_case_id", "d1"),
                                        literal("transactions"),
                                    ),
                                ),
                            ),
                            f.equals(column("granularity", "d1"), literal(60)),
                        ),
                    ),
                    and_cond(
                        f.equals(column("metric_id", "d0"), literal(123456)),
                        f.equals(column("metric_id", "d1"), literal(123456)),
                    ),
                ),
            ),
            groupby=[time_expression("d1"), time_expression("d0")],
            having=None,
            order_by=[OrderBy(OrderByDirection.ASC, time_expression("d0"))],
            limitby=None,
            limit=1000,
            offset=0,
            totals=False,
            granularity=None,
        ),
    ),
    pytest.param(
        (
            "apdex(sum(`d:transactions/duration@millisecond`), 123) / max(`d:transactions/duration@millisecond`)",
            get_dataset("generic_metrics"),
            {
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
            },
            None,
        ),
        CompositeQuery(
            from_clause=join_clause,
            selected_columns=[
                SelectedExpression(
                    "aggregate_value",
                    divide(
                        FunctionCall(
                            None,
                            "apdex",
                            (
                                FunctionCall(
                                    None,
                                    "sum",
                                    (Column(None, "d0", "value"),),
                                ),
                                Literal(None, 123.0),
                            ),
                        ),
                        FunctionCall(
                            None,
                            "max",
                            (Column(None, "d1", "value"),),
                        ),
                        "aggregate_value",
                    ),
                ),
                SelectedExpression(
                    "time",
                    time_expression("d1"),
                ),
                SelectedExpression(
                    "time",
                    time_expression("d0"),
                ),
            ],
            array_join=None,
            condition=and_cond(
                and_cond(
                    and_cond(
                        f.greaterOrEquals(
                            column("timestamp", "d0"),
                            literal(datetime(2023, 11, 23, 18, 30)),
                        ),
                        f.less(
                            column("timestamp", "d0"),
                            literal(datetime(2023, 11, 23, 22, 30)),
                        ),
                    ),
                    and_cond(
                        and_cond(
                            in_cond(column("project_id", "d0"), f.tuple(literal(11))),
                            and_cond(
                                in_cond(column("org_id", "d0"), f.tuple(literal(1))),
                                f.equals(
                                    column("use_case_id", "d0"), literal("transactions")
                                ),
                            ),
                        ),
                        f.equals(column("granularity", "d0"), literal(60)),
                    ),
                ),
                and_cond(
                    and_cond(
                        and_cond(
                            f.greaterOrEquals(
                                column("timestamp", "d1"),
                                literal(datetime(2023, 11, 23, 18, 30)),
                            ),
                            f.less(
                                column("timestamp", "d1"),
                                literal(datetime(2023, 11, 23, 22, 30)),
                            ),
                        ),
                        and_cond(
                            and_cond(
                                in_cond(
                                    column("project_id", "d1"), f.tuple(literal(11))
                                ),
                                and_cond(
                                    in_cond(
                                        column("org_id", "d1"), f.tuple(literal(1))
                                    ),
                                    f.equals(
                                        column("use_case_id", "d1"),
                                        literal("transactions"),
                                    ),
                                ),
                            ),
                            f.equals(column("granularity", "d1"), literal(60)),
                        ),
                    ),
                    and_cond(
                        f.equals(column("metric_id", "d0"), literal(123456)),
                        f.equals(column("metric_id", "d1"), literal(123456)),
                    ),
                ),
            ),
            groupby=[time_expression("d1"), time_expression("d0")],
            having=None,
            order_by=[OrderBy(OrderByDirection.ASC, time_expression("d0"))],
            limitby=None,
            limit=1000,
            offset=0,
            totals=False,
            granularity=None,
        ),
    ),
    pytest.param(
        (
            'sum(`d:transactions/duration@millisecond`){dist:["dist1", "dist2"]} by (transaction, status_code)',
            get_dataset("generic_metrics"),
            {
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
                    "dist": 888,
                    "transaction": 111111,
                    "status_code": 222222,
                },
                "limit": None,
                "offset": None,
            },
            None,
        ),
        Query(
            from_clause=Entity(
                EntityKey.GENERIC_METRICS_DISTRIBUTIONS,
                get_entity(EntityKey.GENERIC_METRICS_DISTRIBUTIONS).get_data_model(),
            ),
            selected_columns=[
                SelectedExpression(
                    "aggregate_value", f.sum(column("value"), alias="aggregate_value")
                ),
                SelectedExpression(
                    "transaction", column("tags_raw[111111]", None, "transaction")
                ),
                SelectedExpression(
                    "status_code", column("tags_raw[222222]", None, "status_code")
                ),
                SelectedExpression(
                    "time",
                    f.toStartOfInterval(
                        column("timestamp"),
                        f.toIntervalSecond(literal(60)),
                        literal("Universal"),
                        alias="time",
                    ),
                ),
            ],
            array_join=None,
            condition=and_cond(
                and_cond(
                    and_cond(
                        f.greaterOrEquals(
                            column("timestamp", None),
                            literal(datetime(2023, 11, 23, 18, 30)),
                        ),
                        f.less(
                            column("timestamp", None),
                            literal(datetime(2023, 11, 23, 22, 30)),
                        ),
                    ),
                    and_cond(
                        and_cond(
                            in_cond(column("project_id", None), f.tuple(literal(11))),
                            and_cond(
                                in_cond(column("org_id", None), f.tuple(literal(1))),
                                f.equals(
                                    column("use_case_id", None), literal("transactions")
                                ),
                            ),
                        ),
                        f.equals(column("granularity", None), literal(60)),
                    ),
                ),
                and_cond(
                    f.equals(column("metric_id", None), literal(123456)),
                    in_cond(
                        column("tags_raw[888]"),
                        f.tuple(literal("dist1"), literal("dist2")),
                    ),
                ),
            ),
            groupby=[
                column("tags_raw[111111]", None, "transaction"),
                column("tags_raw[222222]", None, "status_code"),
                f.toStartOfInterval(
                    column("timestamp"),
                    f.toIntervalSecond(literal(60)),
                    literal("Universal"),
                    alias="time",
                ),
            ],
            having=None,
            order_by=[
                OrderBy(
                    OrderByDirection.ASC,
                    f.toStartOfInterval(
                        column("timestamp"),
                        f.toIntervalSecond(literal(60)),
                        literal("Universal"),
                        alias="time",
                    ),
                )
            ],
            limitby=None,
            limit=1000,
            offset=0,
            totals=False,
            granularity=None,
        ),
    ),
    pytest.param(
        (
            'sum(`d:transactions/duration@millisecond`){dist:["dist1", "dist2"]}',
            get_dataset("generic_metrics"),
            {
                "start": "2021-01-01T00:00:00",
                "end": "2021-01-02T00:00:00",
                "rollup": {
                    "orderby": None,
                    "granularity": 60,
                    "interval": 60,
                    "with_totals": None,
                },
                "scope": {
                    "org_ids": [1],
                    "project_ids": [1],
                    "use_case_id": "transactions",
                },
                "limit": None,
                "offset": None,
                "indexer_mappings": {
                    "d:transactions/duration@millisecond": 123456,
                    "dist": 888,
                },
            },
            None,
        ),
        Query(
            from_clause=Entity(
                EntityKey.GENERIC_METRICS_DISTRIBUTIONS,
                get_entity(EntityKey.GENERIC_METRICS_DISTRIBUTIONS).get_data_model(),
            ),
            selected_columns=[
                SelectedExpression(
                    "aggregate_value", f.sum(column("value"), alias="aggregate_value")
                ),
                SelectedExpression(
                    "time",
                    time_expression(None),
                ),
            ],
            array_join=None,
            condition=and_cond(
                and_cond(
                    and_cond(
                        f.greaterOrEquals(
                            column("timestamp", None),
                            literal(datetime(2021, 1, 1, 0, 0)),
                        ),
                        f.less(
                            column("timestamp", None),
                            literal(datetime(2021, 1, 2, 0, 0)),
                        ),
                    ),
                    and_cond(
                        and_cond(
                            in_cond(column("project_id", None), f.tuple(literal(1))),
                            and_cond(
                                in_cond(column("org_id", None), f.tuple(literal(1))),
                                f.equals(
                                    column("use_case_id", None), literal("transactions")
                                ),
                            ),
                        ),
                        f.equals(column("granularity", None), literal(60)),
                    ),
                ),
                and_cond(
                    f.equals(column("metric_id", None), literal(123456)),
                    in_cond(
                        column("tags_raw[888]"),
                        f.tuple(literal("dist1"), literal("dist2")),
                    ),
                ),
            ),
            groupby=[time_expression(None)],
            having=None,
            order_by=[OrderBy(OrderByDirection.ASC, time_expression(None))],
            limitby=None,
            limit=1000,
            offset=0,
            totals=False,
            granularity=None,
        ),
    ),
    pytest.param(
        (
            "sum(`d:transactions/duration@millisecond`){}",
            get_dataset("generic_metrics"),
            {
                "start": "2021-01-01T00:00:00",
                "end": "2021-01-02T00:00:00",
                "rollup": {
                    "orderby": None,
                    "granularity": 60,
                    "interval": 60,
                    "with_totals": None,
                },
                "scope": {
                    "org_ids": [1],
                    "project_ids": [1],
                    "use_case_id": "transactions",
                },
                "limit": None,
                "offset": None,
                "indexer_mappings": {"d:transactions/duration@millisecond": 123456},
            },
            None,
        ),
        Query(
            from_clause=Entity(
                EntityKey.GENERIC_METRICS_DISTRIBUTIONS,
                get_entity(EntityKey.GENERIC_METRICS_DISTRIBUTIONS).get_data_model(),
            ),
            selected_columns=[
                SelectedExpression(
                    "aggregate_value", f.sum(column("value"), alias="aggregate_value")
                ),
                SelectedExpression(
                    "time",
                    time_expression(None),
                ),
            ],
            array_join=None,
            condition=and_cond(
                and_cond(
                    and_cond(
                        f.greaterOrEquals(
                            column("timestamp", None),
                            literal(datetime(2021, 1, 1, 0, 0)),
                        ),
                        f.less(
                            column("timestamp", None),
                            literal(datetime(2021, 1, 2, 0, 0)),
                        ),
                    ),
                    and_cond(
                        and_cond(
                            in_cond(column("project_id", None), f.tuple(literal(1))),
                            and_cond(
                                in_cond(column("org_id", None), f.tuple(literal(1))),
                                f.equals(
                                    column("use_case_id", None), literal("transactions")
                                ),
                            ),
                        ),
                        f.equals(column("granularity", None), literal(60)),
                    ),
                ),
                f.equals(column("metric_id", None), literal(123456)),
            ),
            groupby=[time_expression(None)],
            having=None,
            order_by=[OrderBy(OrderByDirection.ASC, time_expression(None))],
            limitby=None,
            limit=1000,
            offset=0,
            totals=False,
            granularity=None,
        ),
    ),
    pytest.param(
        (
            'quantiles(0.5, 0.75)(s:transactions/user@none{!dist:["dist1", "dist2"]}){foo: bar} by (transaction)',
            get_dataset("generic_metrics"),
            {
                "start": "2021-01-01T01:36:00",
                "end": "2021-01-05T04:15:00",
                "rollup": {
                    "orderby": None,
                    "granularity": 60,
                    "interval": 60,
                    "with_totals": None,
                },
                "scope": {
                    "org_ids": [1],
                    "project_ids": [1],
                    "use_case_id": "transactions",
                },
                "limit": 100,
                "offset": 3,
                "indexer_mappings": {
                    "transaction.user": "s:transactions/user@none",
                    "s:transactions/user@none": 567890,
                    "dist": 888888,
                    "foo": 777777,
                    "transaction": 111111,
                },
            },
            None,
        ),
        Query(
            from_clause=Entity(
                EntityKey.GENERIC_METRICS_SETS,
                get_entity(EntityKey.GENERIC_METRICS_SETS).get_data_model(),
            ),
            selected_columns=[
                SelectedExpression(
                    "aggregate_value",
                    CurriedFunctionCall(
                        "aggregate_value",
                        f.quantiles(literal(0.5), literal(0.75)),
                        (column("value"),),
                    ),
                ),
                SelectedExpression(
                    "transaction", column("tags_raw[111111]", None, "transaction")
                ),
                SelectedExpression(
                    "time",
                    time_expression(None),
                ),
            ],
            array_join=None,
            condition=and_cond(
                and_cond(
                    and_cond(
                        f.greaterOrEquals(
                            column("timestamp", None),
                            literal(datetime(2021, 1, 1, 1, 36)),
                        ),
                        f.less(
                            column("timestamp", None),
                            literal(datetime(2021, 1, 5, 4, 15)),
                        ),
                    ),
                    and_cond(
                        and_cond(
                            in_cond(column("project_id", None), f.tuple(literal(1))),
                            and_cond(
                                in_cond(column("org_id", None), f.tuple(literal(1))),
                                f.equals(
                                    column("use_case_id", None), literal("transactions")
                                ),
                            ),
                        ),
                        f.equals(column("granularity", None), literal(60)),
                    ),
                ),
                and_cond(
                    f.equals(column("metric_id", None), literal(567890)),
                    and_cond(
                        f.notIn(
                            column("tags_raw[888888]"),
                            f.tuple(literal("dist1"), literal("dist2")),
                        ),
                        f.equals(column("tags_raw[777777]"), literal("bar")),
                    ),
                ),
            ),
            groupby=[
                column("tags_raw[111111]", None, "transaction"),
                time_expression(None),
            ],
            having=None,
            order_by=[OrderBy(OrderByDirection.ASC, time_expression(None))],
            limitby=None,
            limit=100,
            offset=3,
            totals=False,
            granularity=None,
        ),
    ),
    pytest.param(
        (
            'quantiles(0.5)(`d:transactions/duration@millisecond`){dist:["dist1", "dist2"]} by (transaction, status_code)',
            get_dataset("generic_metrics"),
            {
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
                    "dist": 888,
                    "transaction": 111111,
                    "status_code": 222222,
                },
                "limit": None,
                "offset": None,
            },
            None,
        ),
        Query(
            from_clause=Entity(
                EntityKey.GENERIC_METRICS_DISTRIBUTIONS,
                get_entity(EntityKey.GENERIC_METRICS_DISTRIBUTIONS).get_data_model(),
            ),
            selected_columns=[
                SelectedExpression(
                    "aggregate_value",
                    CurriedFunctionCall(
                        "aggregate_value", f.quantiles(literal(0.5)), (column("value"),)
                    ),
                ),
                SelectedExpression(
                    "transaction", column("tags_raw[111111]", None, "transaction")
                ),
                SelectedExpression(
                    "status_code", column("tags_raw[222222]", None, "status_code")
                ),
                SelectedExpression(
                    "time",
                    time_expression(None),
                ),
            ],
            array_join=None,
            condition=and_cond(
                and_cond(
                    and_cond(
                        f.greaterOrEquals(
                            column("timestamp", None),
                            literal(datetime(2023, 11, 23, 18, 30)),
                        ),
                        f.less(
                            column("timestamp", None),
                            literal(datetime(2023, 11, 23, 22, 30)),
                        ),
                    ),
                    and_cond(
                        and_cond(
                            in_cond(column("project_id", None), f.tuple(literal(11))),
                            and_cond(
                                in_cond(column("org_id", None), f.tuple(literal(1))),
                                f.equals(
                                    column("use_case_id", None), literal("transactions")
                                ),
                            ),
                        ),
                        f.equals(column("granularity", None), literal(60)),
                    ),
                ),
                and_cond(
                    f.equals(column("metric_id", None), literal(123456)),
                    in_cond(
                        column("tags_raw[888]"),
                        f.tuple(literal("dist1"), literal("dist2")),
                    ),
                ),
            ),
            groupby=[
                column("tags_raw[111111]", None, "transaction"),
                column("tags_raw[222222]", None, "status_code"),
                time_expression(None),
            ],
            having=None,
            order_by=[
                OrderBy(
                    OrderByDirection.ASC,
                    f.toStartOfInterval(
                        column("timestamp"),
                        f.toIntervalSecond(literal(60)),
                        literal("Universal"),
                        alias="time",
                    ),
                )
            ],
            limitby=None,
            limit=1000,
            offset=0,
            totals=False,
            granularity=None,
        ),
    ),
    pytest.param(
        (
            'sum(`d:sessions/duration@second`){release:["foo", "bar"]} by release',
            get_dataset("metrics"),
            {
                "start": "2021-01-01T00:00:00",
                "end": "2021-01-02T00:00:00",
                "rollup": {
                    "orderby": None,
                    "granularity": 60,
                    "interval": 60,
                    "with_totals": None,
                },
                "scope": {
                    "org_ids": [1],
                    "project_ids": [1],
                    "use_case_id": "sessions",
                },
                "limit": None,
                "offset": None,
                "indexer_mappings": {
                    "d:sessions/duration@second": 123456,
                    "release": 111,
                    "foo": 222,
                    "bar": 333,
                },
            },
            None,
        ),
        Query(
            from_clause=Entity(
                EntityKey.METRICS_DISTRIBUTIONS,
                get_entity(EntityKey.METRICS_DISTRIBUTIONS).get_data_model(),
            ),
            selected_columns=[
                SelectedExpression(
                    "aggregate_value", f.sum(column("value"), alias="aggregate_value")
                ),
                SelectedExpression("release", column("tags[111]", None, "release")),
                SelectedExpression("time", time_expression(None)),
            ],
            array_join=None,
            condition=and_cond(
                and_cond(
                    and_cond(
                        f.greaterOrEquals(
                            column("timestamp", None),
                            literal(datetime(2021, 1, 1, 0, 0)),
                        ),
                        f.less(
                            column("timestamp", None),
                            literal(datetime(2021, 1, 2, 0, 0)),
                        ),
                    ),
                    and_cond(
                        and_cond(
                            in_cond(column("project_id", None), f.tuple(literal(1))),
                            and_cond(
                                in_cond(column("org_id", None), f.tuple(literal(1))),
                                f.equals(
                                    column("use_case_id", None), literal("sessions")
                                ),
                            ),
                        ),
                        f.equals(column("granularity", None), literal(60)),
                    ),
                ),
                and_cond(
                    f.equals(column("metric_id", None), literal(123456)),
                    in_cond(
                        column("tags[111]"),
                        f.tuple(literal(222), literal(333)),
                    ),
                ),
            ),
            groupby=[column("tags[111]", None, "release"), time_expression(None)],
            having=None,
            order_by=[OrderBy(OrderByDirection.ASC, time_expression(None))],
            limitby=None,
            limit=1000,
            offset=0,
            totals=False,
            granularity=None,
        ),
    ),
    pytest.param(
        (
            'max(d:transactions/duration@millisecond){bar:" !\\"#$%&\'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\\\]^_`abcdefghijklmnopqrstuvwxyz{|}~"} by (transaction)',
            get_dataset("generic_metrics"),
            {
                "start": "2024-01-07T13:35:00+00:00",
                "end": "2024-01-08T13:40:00+00:00",
                "indexer_mappings": {
                    "d:transactions/duration@millisecond": 123456,
                    " !\"#$%&'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz{|}~": 78910,
                    "bar": 111213,
                    "transaction": 141516,
                },
                "limit": 10000,
                "offset": None,
                "rollup": {
                    "granularity": 60,
                    "interval": 300,
                    "orderby": None,
                    "with_totals": None,
                },
                "scope": {
                    "org_ids": [1],
                    "project_ids": [1],
                    "use_case_id": "transactions",
                },
            },
            None,
        ),
        Query(
            from_clause=Entity(
                EntityKey.GENERIC_METRICS_DISTRIBUTIONS,
                get_entity(EntityKey.GENERIC_METRICS_DISTRIBUTIONS).get_data_model(),
            ),
            selected_columns=[
                SelectedExpression(
                    "aggregate_value", f.max(column("value"), alias="aggregate_value")
                ),
                SelectedExpression(
                    "transaction", column("tags_raw[141516]", None, "transaction")
                ),
                SelectedExpression(
                    "time",
                    f.toStartOfInterval(
                        column("timestamp"),
                        f.toIntervalSecond(literal(300)),
                        literal("Universal"),
                        alias="time",
                    ),
                ),
            ],
            array_join=None,
            condition=and_cond(
                and_cond(
                    and_cond(
                        f.greaterOrEquals(
                            column("timestamp", None),
                            literal(datetime(2024, 1, 7, 13, 35)),
                        ),
                        f.less(
                            column("timestamp", None),
                            literal(datetime(2024, 1, 8, 13, 40)),
                        ),
                    ),
                    and_cond(
                        and_cond(
                            in_cond(column("project_id", None), f.tuple(literal(1))),
                            and_cond(
                                in_cond(column("org_id", None), f.tuple(literal(1))),
                                f.equals(
                                    column("use_case_id", None), literal("transactions")
                                ),
                            ),
                        ),
                        f.equals(column("granularity", None), literal(60)),
                    ),
                ),
                and_cond(
                    f.equals(column("metric_id", None), literal(123456)),
                    f.equals(
                        column("tags_raw[111213]"),
                        literal(
                            " !\"#$%&'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\\\]^_`abcdefghijklmnopqrstuvwxyz{|}~"
                        ),
                    ),
                ),
            ),
            groupby=[
                column("tags_raw[141516]", None, "transaction"),
                time_expression(None, 300),
            ],
            having=None,
            order_by=[
                OrderBy(
                    OrderByDirection.ASC,
                    time_expression(None, 300),
                )
            ],
            limitby=None,
            limit=10000,
            offset=0,
            totals=False,
            granularity=None,
        ),
    ),
    pytest.param(
        (
            'apdex(sum(`d:transactions/duration@millisecond`), 500){dist:["dist1", "dist2"]}',
            get_dataset("generic_metrics"),
            {
                "start": "2021-01-01T00:00:00",
                "end": "2021-01-02T00:00:00",
                "rollup": {
                    "orderby": None,
                    "granularity": 60,
                    "interval": 60,
                    "with_totals": None,
                },
                "scope": {
                    "org_ids": [1],
                    "project_ids": [1],
                    "use_case_id": "transactions",
                },
                "limit": None,
                "offset": None,
                "indexer_mappings": {
                    "d:transactions/duration@millisecond": 123456,
                    "dist": 888,
                },
            },
            None,
        ),
        Query(
            from_clause=Entity(
                EntityKey.GENERIC_METRICS_DISTRIBUTIONS,
                get_entity(EntityKey.GENERIC_METRICS_DISTRIBUTIONS).get_data_model(),
            ),
            selected_columns=[
                SelectedExpression(
                    "aggregate_value",
                    f.apdex(
                        f.sum(column("value")), literal(500.0), alias="aggregate_value"
                    ),
                ),
                SelectedExpression(
                    "time",
                    time_expression(None),
                ),
            ],
            array_join=None,
            condition=and_cond(
                and_cond(
                    and_cond(
                        f.greaterOrEquals(
                            column("timestamp", None),
                            literal(datetime(2021, 1, 1, 0, 0)),
                        ),
                        f.less(
                            column("timestamp", None),
                            literal(datetime(2021, 1, 2, 0, 0)),
                        ),
                    ),
                    and_cond(
                        and_cond(
                            in_cond(column("project_id", None), f.tuple(literal(1))),
                            and_cond(
                                in_cond(column("org_id", None), f.tuple(literal(1))),
                                f.equals(
                                    column("use_case_id", None), literal("transactions")
                                ),
                            ),
                        ),
                        f.equals(column("granularity", None), literal(60)),
                    ),
                ),
                and_cond(
                    f.equals(column("metric_id", None), literal(123456)),
                    in_cond(
                        column("tags_raw[888]"),
                        f.tuple(literal("dist1"), literal("dist2")),
                    ),
                ),
            ),
            groupby=[time_expression(None)],
            having=None,
            order_by=[OrderBy(OrderByDirection.ASC, time_expression(None))],
            limitby=None,
            limit=1000,
            offset=0,
            totals=False,
            granularity=None,
        ),
    ),
    pytest.param(
        (
            "topK(10)(sum(s:transactions/user@none), 300)",
            get_dataset("generic_metrics"),
            {
                "start": "2021-01-01T01:36:00",
                "end": "2021-01-05T04:15:00",
                "rollup": {
                    "orderby": None,
                    "granularity": 3600,
                    "interval": 3600,
                    "with_totals": None,
                },
                "scope": {
                    "org_ids": [1],
                    "project_ids": [1],
                    "use_case_id": "transactions",
                },
                "limit": 100,
                "offset": 3,
                "indexer_mappings": {
                    "transaction.user": "s:transactions/user@none",
                    "s:transactions/user@none": 567890,
                    "dist": 888888,
                    "foo": 777777,
                    "transaction": 111111,
                },
            },
            None,
        ),
        Query(
            from_clause=Entity(
                EntityKey.GENERIC_METRICS_SETS,
                get_entity(EntityKey.GENERIC_METRICS_SETS).get_data_model(),
            ),
            selected_columns=[
                SelectedExpression(
                    "aggregate_value",
                    CurriedFunctionCall(
                        None,
                        f.topK(literal(10.0)),
                        (
                            f.sum(column("value"), alias="aggregate_value"),
                            literal(300.0),
                        ),
                    ),
                ),
                SelectedExpression("time", time_expression(None, 3600)),
            ],
            array_join=None,
            condition=and_cond(
                and_cond(
                    and_cond(
                        f.greaterOrEquals(
                            column("timestamp", None),
                            literal(datetime(2021, 1, 1, 1, 36)),
                        ),
                        f.less(
                            column("timestamp", None),
                            literal(datetime(2021, 1, 5, 4, 15)),
                        ),
                    ),
                    and_cond(
                        and_cond(
                            in_cond(column("project_id", None), f.tuple(literal(1))),
                            and_cond(
                                in_cond(column("org_id", None), f.tuple(literal(1))),
                                f.equals(
                                    column("use_case_id", None), literal("transactions")
                                ),
                            ),
                        ),
                        f.equals(column("granularity", None), literal(3600)),
                    ),
                ),
                f.equals(column("metric_id", None), literal(567890)),
            ),
            groupby=[time_expression(None, 3600)],
            having=None,
            order_by=[OrderBy(OrderByDirection.ASC, time_expression(None, 3600))],
            limitby=None,
            limit=100,
            offset=3,
            totals=False,
            granularity=None,
        ),
    ),
    pytest.param(
        (
            'avg(d:custom/sentry.event_manager.save_transactions.fetch_organizations@second){(event_type:"transaction" AND transaction:"sentry.tasks.store.save_event_transaction")}',
            get_dataset("generic_metrics"),
            {
                "start": "2021-01-01T00:00:00",
                "end": "2021-01-02T00:00:00",
                "rollup": {
                    "orderby": None,
                    "granularity": 60,
                    "interval": 60,
                    "with_totals": None,
                },
                "scope": {"org_ids": [1], "project_ids": [1], "use_case_id": "custom"},
                "limit": None,
                "offset": None,
                "indexer_mappings": {
                    "d:custom/sentry.event_manager.save_transactions.fetch_organizations@second": 111111,
                    "event_type": 222222,
                    "transaction": 333333,
                },
            },
            None,
        ),
        Query(
            from_clause=Entity(
                EntityKey.GENERIC_METRICS_DISTRIBUTIONS,
                get_entity(EntityKey.GENERIC_METRICS_DISTRIBUTIONS).get_data_model(),
            ),
            selected_columns=[
                SelectedExpression(
                    "aggregate_value", f.avg(column("value"), alias="aggregate_value")
                ),
                SelectedExpression("time", time_expression(None)),
            ],
            array_join=None,
            condition=and_cond(
                and_cond(
                    and_cond(
                        f.greaterOrEquals(
                            column("timestamp", None),
                            literal(datetime(2021, 1, 1, 0, 0)),
                        ),
                        f.less(
                            column("timestamp", None),
                            literal(datetime(2021, 1, 2, 0, 0)),
                        ),
                    ),
                    and_cond(
                        and_cond(
                            in_cond(column("project_id", None), f.tuple(literal(1))),
                            and_cond(
                                in_cond(column("org_id", None), f.tuple(literal(1))),
                                f.equals(
                                    column("use_case_id", None), literal("custom")
                                ),
                            ),
                        ),
                        f.equals(column("granularity", None), literal(60)),
                    ),
                ),
                and_cond(
                    f.equals(column("metric_id"), literal(111111)),
                    and_cond(
                        f.equals(column("tags_raw[222222]"), literal("transaction")),
                        f.equals(
                            column("tags_raw[333333]"),
                            literal("sentry.tasks.store.save_event_transaction"),
                        ),
                    ),
                ),
            ),
            groupby=[time_expression(None)],
            having=None,
            order_by=[OrderBy(OrderByDirection.ASC, time_expression(None))],
            limitby=None,
            limit=1000,
            offset=0,
            totals=False,
            granularity=None,
        ),
    ),
    pytest.param(
        (
            '((avg(d:transactions/duration@millisecond) * 100.0) * 100.0){transaction:"getsentry.tasks.calculate_spike_projections"}',
            get_dataset("generic_metrics"),
            {
                "end": "2024-04-08T06:49:00+00:00",
                "indexer_mappings": {
                    "d:transactions/duration@millisecond": 9223372036854775909,
                    "transaction": 9223372036854776020,
                },
                "limit": 10000,
                "offset": None,
                "rollup": {
                    "granularity": 60,
                    "interval": 60,
                    "orderby": None,
                    "with_totals": None,
                },
                "scope": {
                    "org_ids": [1],
                    "project_ids": [1],
                    "use_case_id": "'transactions'",
                },
                "start": "2024-04-08T05:48:00+00:00",
            },
            None,
        ),
        Query(
            from_clause=Entity(
                EntityKey.GENERIC_METRICS_DISTRIBUTIONS,
                get_entity(EntityKey.GENERIC_METRICS_DISTRIBUTIONS).get_data_model(),
            ),
            selected_columns=[
                SelectedExpression(
                    "aggregate_value",
                    multiply(
                        multiply(
                            FunctionCall(
                                None,
                                "avg",
                                (Column(None, None, "value"),),
                            ),
                            Literal(None, 100.0),
                        ),
                        Literal(None, 100.0),
                        "aggregate_value",
                    ),
                ),
                SelectedExpression("time", time_expression(None)),
            ],
            array_join=None,
            condition=and_cond(
                and_cond(
                    and_cond(
                        f.greaterOrEquals(
                            column("timestamp", None),
                            literal(datetime(2024, 4, 8, 5, 48)),
                        ),
                        f.less(
                            column("timestamp", None),
                            literal(datetime(2024, 4, 8, 6, 49)),
                        ),
                    ),
                    and_cond(
                        and_cond(
                            in_cond(column("project_id", None), f.tuple(literal(1))),
                            and_cond(
                                in_cond(column("org_id", None), f.tuple(literal(1))),
                                f.equals(
                                    column("use_case_id", None),
                                    literal("'transactions'"),
                                ),
                            ),
                        ),
                        f.equals(column("granularity", None), literal(60)),
                    ),
                ),
                and_cond(
                    f.equals(
                        column("tags_raw[9223372036854776020]"),
                        literal("getsentry.tasks.calculate_spike_projections"),
                    ),
                    f.equals(column("metric_id"), literal(9223372036854775909)),
                ),
            ),
            groupby=[time_expression(None)],
            having=None,
            order_by=[OrderBy(OrderByDirection.ASC, time_expression(None))],
            limitby=None,
            limit=10000,
            offset=0,
            totals=False,
            granularity=None,
        ),
    ),
]


@pytest.mark.parametrize("theinput, expected", test_cases)
def test_autogenerated(
    theinput: tuple[str, Dataset, dict[str, Any], QuerySettings | None], expected: Query
) -> None:
    body, dataset, mql_context_dict, settings = theinput
    actual = ParsePopulateResolveMQL().execute(
        # query_settings and timer are dummy and dont matter
        QueryPipelineResult(
            data=(body, dataset, mql_context_dict, settings),
            error=None,
            query_settings=HTTPQuerySettings(),
            timer=Timer("mql_pipeline"),
        )
    )
    assert actual.data and not actual.error
    eq, reason = actual.data.equals(expected)
    assert eq, reason


failure_cases = [
    pytest.param(
        (
            "sum(`d:transactions/duration@millisecond`){status_code:200} by transaction / sum(`d:transactions/duration@millisecond`) by status_code",
            get_dataset("generic_metrics"),
            {
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
            },
            None,
        ),
        ParsingException,
    ),
    pytest.param(
        (
            "apdex(sum(`d:transactions/duration@millisecond`) / max(`d:transactions/duration@millisecond`), 123)",
            get_dataset("generic_metrics"),
            {
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
            },
            None,
        ),
        ParsingException,
    ),
    pytest.param(
        (
            'apdex(sum(`d:transactions/duration@millisecond`) / max(`d:transactions/duration@millisecond`), 500){dist:["dist1", "dist2"]}',
            get_dataset("generic_metrics"),
            {
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
            },
            None,
        ),
        ParsingException,
    ),
    pytest.param(
        (
            'sum(`d:transactions/duration@millisecond`){dist:["dist1", "dist2"]}',
            get_dataset("generic_metrics"),
            {
                "start": "2021-01-01T00:00:00",
                "end": "2021-01-02T00:00:00",
                "rollup": {
                    "orderby": None,
                    "granularity": 60,
                    "interval": 10,
                    "with_totals": None,
                },
                "scope": {
                    "org_ids": [1],
                    "project_ids": [1],
                    "use_case_id": "transactions",
                },
                "limit": None,
                "offset": None,
                "indexer_mappings": {
                    "d:transactions/duration@millisecond": 123456,
                    "dist": 888,
                },
            },
            None,
        ),
        ParsingException,
    ),
    pytest.param(
        (
            'sum(`d:transactions/duration@millisecond`){dist:["dist1", "dist2"]}',
            get_dataset("generic_metrics"),
            {
                "start": "2021-01-01T00:00:00",
                "end": "2021-01-02T00:00:00",
                "rollup": {
                    "orderby": "DESC",
                    "granularity": 60,
                    "interval": 60,
                    "with_totals": None,
                },
                "scope": {
                    "org_ids": [1],
                    "project_ids": [1],
                    "use_case_id": "transactions",
                },
                "limit": None,
                "offset": None,
                "indexer_mappings": {
                    "d:transactions/duration@millisecond": 123456,
                    "dist": 888,
                },
            },
            None,
        ),
        ParsingException,
    ),
    pytest.param(
        (
            'sum(`d:transactions/duration@millisecond`){dist:["dist1", "dist2"]}',
            get_dataset("generic_metrics"),
            {
                "end": "2021-01-02T00:00:00",
                "rollup": {
                    "orderby": None,
                    "granularity": 60,
                    "interval": 60,
                    "with_totals": None,
                },
                "scope": {
                    "org_ids": [1],
                    "project_ids": [1],
                    "use_case_id": "transactions",
                },
                "limit": None,
                "offset": None,
                "indexer_mappings": {
                    "d:transactions/duration@millisecond": 123456,
                    "dist": 888,
                },
            },
            None,
        ),
        ParsingException,
    ),
    pytest.param(
        (
            'sum(`d:transactions/duration@millisecond`){dist:["dist1", "dist2"]}',
            get_dataset("generic_metrics"),
            {
                "start": "2021-01-01T00:00:00",
                "end": "2021-01-02T00:00:00",
                "rollup": {
                    "orderby": None,
                    "granularity": 60,
                    "interval": 60,
                    "with_totals": None,
                },
                "scope": {
                    "org_ids": [1],
                    "project_ids": [1],
                    "use_case_id": "transactions",
                },
                "limit": 1000000,
                "offset": None,
                "indexer_mappings": {
                    "d:transactions/duration@millisecond": 123456,
                    "dist": 888,
                },
            },
            None,
        ),
        ParsingException,
    ),
    pytest.param(
        (
            'sum(`transaction.duration`){dist:["dist1", "dist2"]}',
            get_dataset("generic_metrics"),
            {
                "start": "2021-01-01T00:00:00",
                "end": "2021-01-02T00:00:00",
                "rollup": {
                    "orderby": None,
                    "granularity": 60,
                    "interval": 60,
                    "with_totals": None,
                },
                "scope": {
                    "org_ids": [1],
                    "project_ids": [1],
                    "use_case_id": "transactions",
                },
                "limit": 1000000,
                "offset": None,
                "indexer_mappings": {
                    "d:transactions/duration@millisecond": 123456,
                    "dist": 888,
                },
            },
            None,
        ),
        ParsingException,
    ),
    pytest.param(
        (
            "sum(`transaction.duration",
            get_dataset("generic_metrics"),
            {
                "start": "2021-01-01T00:00:00",
                "end": "2021-01-02T00:00:00",
                "rollup": {
                    "orderby": None,
                    "granularity": 60,
                    "interval": 60,
                    "with_totals": None,
                },
                "scope": {
                    "org_ids": [1],
                    "project_ids": [1],
                    "use_case_id": "transactions",
                },
                "limit": 1000000,
                "offset": None,
                "indexer_mappings": {
                    "d:transactions/duration@millisecond": 123456,
                    "dist": 888,
                },
            },
            None,
        ),
        ParsingException,
    ),
]


@pytest.mark.parametrize("theinput, expected_error", failure_cases)
def test_autogenerated_invalid(
    theinput: tuple[str, Dataset, dict[str, Any], QuerySettings | None],
    expected_error: Exception,
) -> None:
    body, dataset, mql_context_dict, settings = theinput
    actual = ParsePopulateResolveMQL().execute(
        # query_settings and timer are dummy and dont matter
        QueryPipelineResult(
            data=(body, dataset, mql_context_dict, settings),
            error=None,
            query_settings=HTTPQuerySettings(),
            timer=Timer("mql_pipeline"),
        )
    )
    assert actual.error and not actual.data
    assert isinstance(type(actual.error), type(expected_error))
