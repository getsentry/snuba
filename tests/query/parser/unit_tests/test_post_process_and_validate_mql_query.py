"""
These tests were auto-generated, many of them may be unnecessary or redundant, feel free to remove some.
This tests the final stage of the MQL parsing pipeline, which looks like AST->AST.
"""

from datetime import datetime

import pytest

from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.pipeline.query_pipeline import QueryPipelineResult
from snuba.query import OrderBy, OrderByDirection, SelectedExpression
from snuba.query.composite import CompositeQuery
from snuba.query.data_source.simple import Entity
from snuba.query.dsl import Functions as f
from snuba.query.dsl import NestedColumn, and_cond, column, in_cond, literal, or_cond
from snuba.query.expressions import CurriedFunctionCall
from snuba.query.logical import Query
from snuba.query.mql.parser import PostProcessAndValidateMQLQuery
from snuba.query.query_settings import HTTPQuerySettings, QuerySettings
from snuba.query.snql.parser import CustomProcessors
from snuba.utils.metrics.timer import Timer

tags = NestedColumn("tags")
tags_raw = NestedColumn("tags_raw")

test_cases = [
    pytest.param(
        (
            Query(
                from_clause=Entity(
                    EntityKey.GENERIC_METRICS_DISTRIBUTIONS,
                    get_entity(
                        EntityKey.GENERIC_METRICS_DISTRIBUTIONS
                    ).get_data_model(),
                ),
                selected_columns=[
                    SelectedExpression(
                        "aggregate_value",
                        f.divide(
                            f.sumIf(
                                column("value"),
                                and_cond(
                                    f.equals(
                                        column("tags_raw[222222]"), literal("200")
                                    ),
                                    f.equals(column("metric_id"), literal(123456)),
                                ),
                            ),
                            f.sumIf(
                                column("value"),
                                f.equals(column("metric_id"), literal(123456)),
                            ),
                            alias="aggregate_value",
                        ),
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
                    f.equals(column("granularity"), literal(60)),
                    and_cond(
                        and_cond(
                            in_cond(column("project_id"), f.tuple(literal(11))),
                            and_cond(
                                in_cond(column("org_id"), f.tuple(literal(1))),
                                f.equals(
                                    column("use_case_id"), literal("transactions")
                                ),
                            ),
                        ),
                        and_cond(
                            f.greaterOrEquals(
                                column("timestamp"),
                                literal(datetime(2023, 11, 23, 18, 30)),
                            ),
                            f.less(
                                column("timestamp"),
                                literal(datetime(2023, 11, 23, 22, 30)),
                            ),
                        ),
                    ),
                ),
                groupby=[
                    f.toStartOfInterval(
                        column("timestamp"),
                        f.toIntervalSecond(literal(60)),
                        literal("Universal"),
                        alias="time",
                    )
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
            None,
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
                    f.divide(
                        f.sumIf(
                            column("value", None, "_snuba_value"),
                            and_cond(
                                f.equals(tags_raw["222222"], literal("200")),
                                f.equals(
                                    column("metric_id", None, "_snuba_metric_id"),
                                    literal(123456),
                                ),
                            ),
                        ),
                        f.sumIf(
                            column("value", None, "_snuba_value"),
                            f.equals(
                                column("metric_id", None, "_snuba_metric_id"),
                                literal(123456),
                            ),
                        ),
                        alias="_snuba_aggregate_value",
                    ),
                ),
                SelectedExpression(
                    "time",
                    f.toStartOfInterval(
                        column("timestamp", None, "_snuba_timestamp"),
                        f.toIntervalSecond(literal(60)),
                        literal("Universal"),
                        alias="_snuba_time",
                    ),
                ),
            ],
            array_join=None,
            condition=and_cond(
                and_cond(
                    or_cond(
                        and_cond(
                            f.equals(tags_raw["222222"], literal("200")),
                            f.equals(
                                column("metric_id", None, "_snuba_metric_id"),
                                literal(123456),
                            ),
                        ),
                        f.equals(
                            column("metric_id", None, "_snuba_metric_id"),
                            literal(123456),
                        ),
                    ),
                    and_cond(
                        f.equals(
                            column("granularity", None, "_snuba_granularity"),
                            literal(60),
                        ),
                        in_cond(
                            column("project_id", None, "_snuba_project_id"),
                            f.tuple(literal(11)),
                        ),
                    ),
                ),
                and_cond(
                    and_cond(
                        in_cond(
                            column("org_id", None, "_snuba_org_id"), f.tuple(literal(1))
                        ),
                        f.equals(
                            column("use_case_id", None, "_snuba_use_case_id"),
                            literal("transactions"),
                        ),
                    ),
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
                ),
            ),
            groupby=[
                f.toStartOfInterval(
                    column("timestamp", None, "_snuba_timestamp"),
                    f.toIntervalSecond(literal(60)),
                    literal("Universal"),
                    alias="_snuba_time",
                )
            ],
            having=None,
            order_by=[
                OrderBy(
                    OrderByDirection.ASC,
                    f.toStartOfInterval(
                        column("timestamp", None, "_snuba_timestamp"),
                        f.toIntervalSecond(literal(60)),
                        literal("Universal"),
                        alias="_snuba_time",
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
            Query(
                from_clause=Entity(
                    EntityKey.GENERIC_METRICS_DISTRIBUTIONS,
                    get_entity(
                        EntityKey.GENERIC_METRICS_DISTRIBUTIONS
                    ).get_data_model(),
                ),
                selected_columns=[
                    SelectedExpression(
                        "aggregate_value",
                        f.plus(
                            literal(1.0),
                            f.divide(
                                f.sumIf(
                                    column("value"),
                                    and_cond(
                                        f.equals(
                                            column("tags_raw[222222]"), literal("200")
                                        ),
                                        f.equals(column("metric_id"), literal(123456)),
                                    ),
                                ),
                                f.sumIf(
                                    column("value"),
                                    f.equals(column("metric_id"), literal(123456)),
                                ),
                            ),
                            alias="aggregate_value",
                        ),
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
                    f.equals(column("granularity"), literal(60)),
                    and_cond(
                        and_cond(
                            in_cond(column("project_id"), f.tuple(literal(11))),
                            and_cond(
                                in_cond(column("org_id"), f.tuple(literal(1))),
                                f.equals(
                                    column("use_case_id"), literal("transactions")
                                ),
                            ),
                        ),
                        and_cond(
                            f.greaterOrEquals(
                                column("timestamp"),
                                literal(datetime(2023, 11, 23, 18, 30)),
                            ),
                            f.less(
                                column("timestamp"),
                                literal(datetime(2023, 11, 23, 22, 30)),
                            ),
                        ),
                    ),
                ),
                groupby=[
                    f.toStartOfInterval(
                        column("timestamp"),
                        f.toIntervalSecond(literal(60)),
                        literal("Universal"),
                        alias="time",
                    )
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
            None,
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
                    f.plus(
                        literal(1.0),
                        f.divide(
                            f.sumIf(
                                column("value", None, "_snuba_value"),
                                and_cond(
                                    f.equals(tags_raw["222222"], literal("200")),
                                    f.equals(
                                        column("metric_id", None, "_snuba_metric_id"),
                                        literal(123456),
                                    ),
                                ),
                            ),
                            f.sumIf(
                                column("value", None, "_snuba_value"),
                                f.equals(
                                    column("metric_id", None, "_snuba_metric_id"),
                                    literal(123456),
                                ),
                            ),
                        ),
                        alias="_snuba_aggregate_value",
                    ),
                ),
                SelectedExpression(
                    "time",
                    f.toStartOfInterval(
                        column("timestamp", None, "_snuba_timestamp"),
                        f.toIntervalSecond(literal(60)),
                        literal("Universal"),
                        alias="_snuba_time",
                    ),
                ),
            ],
            array_join=None,
            condition=and_cond(
                and_cond(
                    or_cond(
                        and_cond(
                            f.equals(tags_raw["222222"], literal("200")),
                            f.equals(
                                column("metric_id", None, "_snuba_metric_id"),
                                literal(123456),
                            ),
                        ),
                        f.equals(
                            column("metric_id", None, "_snuba_metric_id"),
                            literal(123456),
                        ),
                    ),
                    and_cond(
                        f.equals(
                            column("granularity", None, "_snuba_granularity"),
                            literal(60),
                        ),
                        in_cond(
                            column("project_id", None, "_snuba_project_id"),
                            f.tuple(literal(11)),
                        ),
                    ),
                ),
                and_cond(
                    and_cond(
                        in_cond(
                            column("org_id", None, "_snuba_org_id"), f.tuple(literal(1))
                        ),
                        f.equals(
                            column("use_case_id", None, "_snuba_use_case_id"),
                            literal("transactions"),
                        ),
                    ),
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
                ),
            ),
            groupby=[
                f.toStartOfInterval(
                    column("timestamp", None, "_snuba_timestamp"),
                    f.toIntervalSecond(literal(60)),
                    literal("Universal"),
                    alias="_snuba_time",
                )
            ],
            having=None,
            order_by=[
                OrderBy(
                    OrderByDirection.ASC,
                    f.toStartOfInterval(
                        column("timestamp", None, "_snuba_timestamp"),
                        f.toIntervalSecond(literal(60)),
                        literal("Universal"),
                        alias="_snuba_time",
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
            Query(
                from_clause=Entity(
                    EntityKey.GENERIC_METRICS_DISTRIBUTIONS,
                    get_entity(
                        EntityKey.GENERIC_METRICS_DISTRIBUTIONS
                    ).get_data_model(),
                ),
                selected_columns=[
                    SelectedExpression(
                        "aggregate_value",
                        f.divide(
                            f.sumIf(
                                column("value"),
                                and_cond(
                                    f.equals(
                                        column("tags_raw[222222]"), literal("200")
                                    ),
                                    f.equals(column("metric_id"), literal(123456)),
                                ),
                            ),
                            f.sumIf(
                                column("value"),
                                f.equals(column("metric_id"), literal(123456)),
                            ),
                            alias="aggregate_value",
                        ),
                    ),
                    SelectedExpression(
                        "transaction", column("tags_raw[333333]", None, "transaction")
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
                    f.equals(column("granularity"), literal(60)),
                    and_cond(
                        and_cond(
                            in_cond(column("project_id"), f.tuple(literal(11))),
                            and_cond(
                                in_cond(column("org_id"), f.tuple(literal(1))),
                                f.equals(
                                    column("use_case_id"), literal("transactions")
                                ),
                            ),
                        ),
                        and_cond(
                            f.greaterOrEquals(
                                column("timestamp"),
                                literal(datetime(2023, 11, 23, 18, 30)),
                            ),
                            f.less(
                                column("timestamp"),
                                literal(datetime(2023, 11, 23, 22, 30)),
                            ),
                        ),
                    ),
                ),
                groupby=[
                    column("tags_raw[333333]", None, "transaction"),
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
            None,
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
                    f.divide(
                        f.sumIf(
                            column("value", None, "_snuba_value"),
                            and_cond(
                                f.equals(tags_raw["222222"], literal("200")),
                                f.equals(
                                    column("metric_id", None, "_snuba_metric_id"),
                                    literal(123456),
                                ),
                            ),
                        ),
                        f.sumIf(
                            column("value", None, "_snuba_value"),
                            f.equals(
                                column("metric_id", None, "_snuba_metric_id"),
                                literal(123456),
                            ),
                        ),
                        alias="_snuba_aggregate_value",
                    ),
                ),
                SelectedExpression("transaction", tags_raw["333333"]),
                SelectedExpression(
                    "time",
                    f.toStartOfInterval(
                        column("timestamp", None, "_snuba_timestamp"),
                        f.toIntervalSecond(literal(60)),
                        literal("Universal"),
                        alias="_snuba_time",
                    ),
                ),
            ],
            array_join=None,
            condition=and_cond(
                and_cond(
                    or_cond(
                        and_cond(
                            f.equals(tags_raw["222222"], literal("200")),
                            f.equals(
                                column("metric_id", None, "_snuba_metric_id"),
                                literal(123456),
                            ),
                        ),
                        f.equals(
                            column("metric_id", None, "_snuba_metric_id"),
                            literal(123456),
                        ),
                    ),
                    and_cond(
                        f.equals(
                            column("granularity", None, "_snuba_granularity"),
                            literal(60),
                        ),
                        in_cond(
                            column("project_id", None, "_snuba_project_id"),
                            f.tuple(literal(11)),
                        ),
                    ),
                ),
                and_cond(
                    and_cond(
                        in_cond(
                            column("org_id", None, "_snuba_org_id"), f.tuple(literal(1))
                        ),
                        f.equals(
                            column("use_case_id", None, "_snuba_use_case_id"),
                            literal("transactions"),
                        ),
                    ),
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
                ),
            ),
            groupby=[
                tags_raw["333333"],
                f.toStartOfInterval(
                    column("timestamp", None, "_snuba_timestamp"),
                    f.toIntervalSecond(literal(60)),
                    literal("Universal"),
                    alias="_snuba_time",
                ),
            ],
            having=None,
            order_by=[
                OrderBy(
                    OrderByDirection.ASC,
                    f.toStartOfInterval(
                        column("timestamp", None, "_snuba_timestamp"),
                        f.toIntervalSecond(literal(60)),
                        literal("Universal"),
                        alias="_snuba_time",
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
            Query(
                from_clause=Entity(
                    EntityKey.GENERIC_METRICS_DISTRIBUTIONS,
                    get_entity(
                        EntityKey.GENERIC_METRICS_DISTRIBUTIONS
                    ).get_data_model(),
                ),
                selected_columns=[
                    SelectedExpression(
                        "aggregate_value",
                        f.divide(
                            CurriedFunctionCall(
                                None,
                                f.quantilesIf(literal(0.5)),
                                (
                                    column("value"),
                                    and_cond(
                                        f.equals(
                                            column("tags_raw[222222]"), literal("200")
                                        ),
                                        f.equals(column("metric_id"), literal(123456)),
                                    ),
                                ),
                            ),
                            f.sumIf(
                                column("value"),
                                f.equals(column("metric_id"), literal(123456)),
                            ),
                            alias="aggregate_value",
                        ),
                    ),
                    SelectedExpression(
                        "transaction", column("tags_raw[333333]", None, "transaction")
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
                    f.equals(column("granularity"), literal(60)),
                    and_cond(
                        and_cond(
                            in_cond(column("project_id"), f.tuple(literal(11))),
                            and_cond(
                                in_cond(column("org_id"), f.tuple(literal(1))),
                                f.equals(
                                    column("use_case_id"), literal("transactions")
                                ),
                            ),
                        ),
                        and_cond(
                            f.greaterOrEquals(
                                column("timestamp"),
                                literal(datetime(2023, 11, 23, 18, 30)),
                            ),
                            f.less(
                                column("timestamp"),
                                literal(datetime(2023, 11, 23, 22, 30)),
                            ),
                        ),
                    ),
                ),
                groupby=[
                    column("tags_raw[333333]", None, "transaction"),
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
            None,
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
                    f.divide(
                        f.arrayElement(
                            CurriedFunctionCall(
                                None,
                                f.quantilesIf(literal(0.5)),
                                (
                                    column("value", None, "_snuba_value"),
                                    and_cond(
                                        f.equals(tags_raw["222222"], literal("200")),
                                        f.equals(
                                            column(
                                                "metric_id", None, "_snuba_metric_id"
                                            ),
                                            literal(123456),
                                        ),
                                    ),
                                ),
                            ),
                            literal(1),
                        ),
                        f.sumIf(
                            column("value", None, "_snuba_value"),
                            f.equals(
                                column("metric_id", None, "_snuba_metric_id"),
                                literal(123456),
                            ),
                        ),
                        alias="_snuba_aggregate_value",
                    ),
                ),
                SelectedExpression("transaction", tags_raw["333333"]),
                SelectedExpression(
                    "time",
                    f.toStartOfInterval(
                        column("timestamp", None, "_snuba_timestamp"),
                        f.toIntervalSecond(literal(60)),
                        literal("Universal"),
                        alias="_snuba_time",
                    ),
                ),
            ],
            array_join=None,
            condition=and_cond(
                and_cond(
                    or_cond(
                        and_cond(
                            f.equals(tags_raw["222222"], literal("200")),
                            f.equals(
                                column("metric_id", None, "_snuba_metric_id"),
                                literal(123456),
                            ),
                        ),
                        f.equals(
                            column("metric_id", None, "_snuba_metric_id"),
                            literal(123456),
                        ),
                    ),
                    and_cond(
                        f.equals(
                            column("granularity", None, "_snuba_granularity"),
                            literal(60),
                        ),
                        in_cond(
                            column("project_id", None, "_snuba_project_id"),
                            f.tuple(literal(11)),
                        ),
                    ),
                ),
                and_cond(
                    and_cond(
                        in_cond(
                            column("org_id", None, "_snuba_org_id"), f.tuple(literal(1))
                        ),
                        f.equals(
                            column("use_case_id", None, "_snuba_use_case_id"),
                            literal("transactions"),
                        ),
                    ),
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
                ),
            ),
            groupby=[
                tags_raw["333333"],
                f.toStartOfInterval(
                    column("timestamp", None, "_snuba_timestamp"),
                    f.toIntervalSecond(literal(60)),
                    literal("Universal"),
                    alias="_snuba_time",
                ),
            ],
            having=None,
            order_by=[
                OrderBy(
                    OrderByDirection.ASC,
                    f.toStartOfInterval(
                        column("timestamp", None, "_snuba_timestamp"),
                        f.toIntervalSecond(literal(60)),
                        literal("Universal"),
                        alias="_snuba_time",
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
            Query(
                from_clause=Entity(
                    EntityKey.GENERIC_METRICS_DISTRIBUTIONS,
                    get_entity(
                        EntityKey.GENERIC_METRICS_DISTRIBUTIONS
                    ).get_data_model(),
                ),
                selected_columns=[
                    SelectedExpression(
                        "aggregate_value",
                        f.divide(
                            f.sumIf(
                                column("value"),
                                f.equals(column("metric_id"), literal(123456)),
                            ),
                            f.multiply(
                                f.plus(
                                    f.maxIf(
                                        column("value"),
                                        f.equals(column("metric_id"), literal(123456)),
                                    ),
                                    f.avgIf(
                                        column("value"),
                                        f.equals(column("metric_id"), literal(123456)),
                                    ),
                                ),
                                f.minIf(
                                    column("value"),
                                    f.equals(column("metric_id"), literal(123456)),
                                ),
                            ),
                            alias="aggregate_value",
                        ),
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
                    f.equals(column("granularity"), literal(60)),
                    and_cond(
                        and_cond(
                            in_cond(column("project_id"), f.tuple(literal(11))),
                            and_cond(
                                in_cond(column("org_id"), f.tuple(literal(1))),
                                f.equals(
                                    column("use_case_id"), literal("transactions")
                                ),
                            ),
                        ),
                        and_cond(
                            f.greaterOrEquals(
                                column("timestamp"),
                                literal(datetime(2023, 11, 23, 18, 30)),
                            ),
                            f.less(
                                column("timestamp"),
                                literal(datetime(2023, 11, 23, 22, 30)),
                            ),
                        ),
                    ),
                ),
                groupby=[
                    f.toStartOfInterval(
                        column("timestamp"),
                        f.toIntervalSecond(literal(60)),
                        literal("Universal"),
                        alias="time",
                    )
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
            None,
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
                    f.divide(
                        f.sumIf(
                            column("value", None, "_snuba_value"),
                            f.equals(
                                column("metric_id", None, "_snuba_metric_id"),
                                literal(123456),
                            ),
                        ),
                        f.multiply(
                            f.plus(
                                f.maxIf(
                                    column("value", None, "_snuba_value"),
                                    f.equals(
                                        column("metric_id", None, "_snuba_metric_id"),
                                        literal(123456),
                                    ),
                                ),
                                f.avgIf(
                                    column("value", None, "_snuba_value"),
                                    f.equals(
                                        column("metric_id", None, "_snuba_metric_id"),
                                        literal(123456),
                                    ),
                                ),
                            ),
                            f.minIf(
                                column("value", None, "_snuba_value"),
                                f.equals(
                                    column("metric_id", None, "_snuba_metric_id"),
                                    literal(123456),
                                ),
                            ),
                        ),
                        alias="_snuba_aggregate_value",
                    ),
                ),
                SelectedExpression(
                    "time",
                    f.toStartOfInterval(
                        column("timestamp", None, "_snuba_timestamp"),
                        f.toIntervalSecond(literal(60)),
                        literal("Universal"),
                        alias="_snuba_time",
                    ),
                ),
            ],
            array_join=None,
            condition=and_cond(
                and_cond(
                    or_cond(
                        or_cond(
                            or_cond(
                                f.equals(
                                    column("metric_id", None, "_snuba_metric_id"),
                                    literal(123456),
                                ),
                                f.equals(
                                    column("metric_id", None, "_snuba_metric_id"),
                                    literal(123456),
                                ),
                            ),
                            f.equals(
                                column("metric_id", None, "_snuba_metric_id"),
                                literal(123456),
                            ),
                        ),
                        f.equals(
                            column("metric_id", None, "_snuba_metric_id"),
                            literal(123456),
                        ),
                    ),
                    and_cond(
                        f.equals(
                            column("granularity", None, "_snuba_granularity"),
                            literal(60),
                        ),
                        in_cond(
                            column("project_id", None, "_snuba_project_id"),
                            f.tuple(literal(11)),
                        ),
                    ),
                ),
                and_cond(
                    and_cond(
                        in_cond(
                            column("org_id", None, "_snuba_org_id"), f.tuple(literal(1))
                        ),
                        f.equals(
                            column("use_case_id", None, "_snuba_use_case_id"),
                            literal("transactions"),
                        ),
                    ),
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
                ),
            ),
            groupby=[
                f.toStartOfInterval(
                    column("timestamp", None, "_snuba_timestamp"),
                    f.toIntervalSecond(literal(60)),
                    literal("Universal"),
                    alias="_snuba_time",
                )
            ],
            having=None,
            order_by=[
                OrderBy(
                    OrderByDirection.ASC,
                    f.toStartOfInterval(
                        column("timestamp", None, "_snuba_timestamp"),
                        f.toIntervalSecond(literal(60)),
                        literal("Universal"),
                        alias="_snuba_time",
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
            Query(
                from_clause=Entity(
                    EntityKey.GENERIC_METRICS_DISTRIBUTIONS,
                    get_entity(
                        EntityKey.GENERIC_METRICS_DISTRIBUTIONS
                    ).get_data_model(),
                ),
                selected_columns=[
                    SelectedExpression(
                        "aggregate_value",
                        f.divide(
                            f.sumIf(
                                column("value"),
                                and_cond(
                                    f.equals(
                                        column("tags_raw[222222]"), literal("200")
                                    ),
                                    f.equals(column("metric_id"), literal(123456)),
                                ),
                            ),
                            f.maxIf(
                                column("value"),
                                and_cond(
                                    f.equals(
                                        column("tags_raw[222222]"), literal("200")
                                    ),
                                    f.equals(column("metric_id"), literal(123456)),
                                ),
                            ),
                            alias="aggregate_value",
                        ),
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
                    f.equals(column("granularity"), literal(60)),
                    and_cond(
                        and_cond(
                            in_cond(column("project_id"), f.tuple(literal(11))),
                            and_cond(
                                in_cond(column("org_id"), f.tuple(literal(1))),
                                f.equals(
                                    column("use_case_id"), literal("transactions")
                                ),
                            ),
                        ),
                        and_cond(
                            f.greaterOrEquals(
                                column("timestamp"),
                                literal(datetime(2023, 11, 23, 18, 30)),
                            ),
                            f.less(
                                column("timestamp"),
                                literal(datetime(2023, 11, 23, 22, 30)),
                            ),
                        ),
                    ),
                ),
                groupby=[
                    f.toStartOfInterval(
                        column("timestamp"),
                        f.toIntervalSecond(literal(60)),
                        literal("Universal"),
                        alias="time",
                    )
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
            None,
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
                    f.divide(
                        f.sumIf(
                            column("value", None, "_snuba_value"),
                            and_cond(
                                f.equals(tags_raw["222222"], literal("200")),
                                f.equals(
                                    column("metric_id", None, "_snuba_metric_id"),
                                    literal(123456),
                                ),
                            ),
                        ),
                        f.maxIf(
                            column("value", None, "_snuba_value"),
                            and_cond(
                                f.equals(tags_raw["222222"], literal("200")),
                                f.equals(
                                    column("metric_id", None, "_snuba_metric_id"),
                                    literal(123456),
                                ),
                            ),
                        ),
                        alias="_snuba_aggregate_value",
                    ),
                ),
                SelectedExpression(
                    "time",
                    f.toStartOfInterval(
                        column("timestamp", None, "_snuba_timestamp"),
                        f.toIntervalSecond(literal(60)),
                        literal("Universal"),
                        alias="_snuba_time",
                    ),
                ),
            ],
            array_join=None,
            condition=and_cond(
                and_cond(
                    or_cond(
                        and_cond(
                            f.equals(tags_raw["222222"], literal("200")),
                            f.equals(
                                column("metric_id", None, "_snuba_metric_id"),
                                literal(123456),
                            ),
                        ),
                        and_cond(
                            f.equals(tags_raw["222222"], literal("200")),
                            f.equals(
                                column("metric_id", None, "_snuba_metric_id"),
                                literal(123456),
                            ),
                        ),
                    ),
                    and_cond(
                        f.equals(
                            column("granularity", None, "_snuba_granularity"),
                            literal(60),
                        ),
                        in_cond(
                            column("project_id", None, "_snuba_project_id"),
                            f.tuple(literal(11)),
                        ),
                    ),
                ),
                and_cond(
                    and_cond(
                        in_cond(
                            column("org_id", None, "_snuba_org_id"), f.tuple(literal(1))
                        ),
                        f.equals(
                            column("use_case_id", None, "_snuba_use_case_id"),
                            literal("transactions"),
                        ),
                    ),
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
                ),
            ),
            groupby=[
                f.toStartOfInterval(
                    column("timestamp", None, "_snuba_timestamp"),
                    f.toIntervalSecond(literal(60)),
                    literal("Universal"),
                    alias="_snuba_time",
                )
            ],
            having=None,
            order_by=[
                OrderBy(
                    OrderByDirection.ASC,
                    f.toStartOfInterval(
                        column("timestamp", None, "_snuba_timestamp"),
                        f.toIntervalSecond(literal(60)),
                        literal("Universal"),
                        alias="_snuba_time",
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
            Query(
                from_clause=Entity(
                    EntityKey.GENERIC_METRICS_DISTRIBUTIONS,
                    get_entity(
                        EntityKey.GENERIC_METRICS_DISTRIBUTIONS
                    ).get_data_model(),
                ),
                selected_columns=[
                    SelectedExpression(
                        "aggregate_value",
                        f.divide(
                            f.sumIf(
                                column("value"),
                                and_cond(
                                    f.equals(
                                        column("tags_raw[222222]"), literal("200")
                                    ),
                                    f.equals(column("metric_id"), literal(123456)),
                                ),
                            ),
                            f.maxIf(
                                column("value"),
                                and_cond(
                                    f.equals(
                                        column("tags_raw[222222]"), literal("200")
                                    ),
                                    f.equals(column("metric_id"), literal(123456)),
                                ),
                            ),
                            alias="aggregate_value",
                        ),
                    ),
                    SelectedExpression(
                        "transaction", column("tags_raw[333333]", None, "transaction")
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
                    f.equals(column("granularity"), literal(60)),
                    and_cond(
                        and_cond(
                            in_cond(column("project_id"), f.tuple(literal(11))),
                            and_cond(
                                in_cond(column("org_id"), f.tuple(literal(1))),
                                f.equals(
                                    column("use_case_id"), literal("transactions")
                                ),
                            ),
                        ),
                        and_cond(
                            f.greaterOrEquals(
                                column("timestamp"),
                                literal(datetime(2023, 11, 23, 18, 30)),
                            ),
                            f.less(
                                column("timestamp"),
                                literal(datetime(2023, 11, 23, 22, 30)),
                            ),
                        ),
                    ),
                ),
                groupby=[
                    column("tags_raw[333333]", None, "transaction"),
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
            None,
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
                    f.divide(
                        f.sumIf(
                            column("value", None, "_snuba_value"),
                            and_cond(
                                f.equals(tags_raw["222222"], literal("200")),
                                f.equals(
                                    column("metric_id", None, "_snuba_metric_id"),
                                    literal(123456),
                                ),
                            ),
                        ),
                        f.maxIf(
                            column("value", None, "_snuba_value"),
                            and_cond(
                                f.equals(tags_raw["222222"], literal("200")),
                                f.equals(
                                    column("metric_id", None, "_snuba_metric_id"),
                                    literal(123456),
                                ),
                            ),
                        ),
                        alias="_snuba_aggregate_value",
                    ),
                ),
                SelectedExpression("transaction", tags_raw["333333"]),
                SelectedExpression(
                    "time",
                    f.toStartOfInterval(
                        column("timestamp", None, "_snuba_timestamp"),
                        f.toIntervalSecond(literal(60)),
                        literal("Universal"),
                        alias="_snuba_time",
                    ),
                ),
            ],
            array_join=None,
            condition=and_cond(
                and_cond(
                    or_cond(
                        and_cond(
                            f.equals(tags_raw["222222"], literal("200")),
                            f.equals(
                                column("metric_id", None, "_snuba_metric_id"),
                                literal(123456),
                            ),
                        ),
                        and_cond(
                            f.equals(tags_raw["222222"], literal("200")),
                            f.equals(
                                column("metric_id", None, "_snuba_metric_id"),
                                literal(123456),
                            ),
                        ),
                    ),
                    and_cond(
                        f.equals(
                            column("granularity", None, "_snuba_granularity"),
                            literal(60),
                        ),
                        in_cond(
                            column("project_id", None, "_snuba_project_id"),
                            f.tuple(literal(11)),
                        ),
                    ),
                ),
                and_cond(
                    and_cond(
                        in_cond(
                            column("org_id", None, "_snuba_org_id"), f.tuple(literal(1))
                        ),
                        f.equals(
                            column("use_case_id", None, "_snuba_use_case_id"),
                            literal("transactions"),
                        ),
                    ),
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
                ),
            ),
            groupby=[
                tags_raw["333333"],
                f.toStartOfInterval(
                    column("timestamp", None, "_snuba_timestamp"),
                    f.toIntervalSecond(literal(60)),
                    literal("Universal"),
                    alias="_snuba_time",
                ),
            ],
            having=None,
            order_by=[
                OrderBy(
                    OrderByDirection.ASC,
                    f.toStartOfInterval(
                        column("timestamp", None, "_snuba_timestamp"),
                        f.toIntervalSecond(literal(60)),
                        literal("Universal"),
                        alias="_snuba_time",
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
            Query(
                from_clause=Entity(
                    EntityKey.GENERIC_METRICS_DISTRIBUTIONS,
                    get_entity(
                        EntityKey.GENERIC_METRICS_DISTRIBUTIONS
                    ).get_data_model(),
                ),
                selected_columns=[
                    SelectedExpression(
                        "aggregate_value",
                        f.plus(
                            f.divide(
                                f.sumIf(
                                    column("value"),
                                    f.equals(column("metric_id"), literal(123456)),
                                ),
                                f.sumIf(
                                    column("value"),
                                    f.equals(column("metric_id"), literal(123456)),
                                ),
                            ),
                            literal(100.0),
                            alias="aggregate_value",
                        ),
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
                    f.equals(column("granularity"), literal(60)),
                    and_cond(
                        and_cond(
                            in_cond(column("project_id"), f.tuple(literal(11))),
                            and_cond(
                                in_cond(column("org_id"), f.tuple(literal(1))),
                                f.equals(
                                    column("use_case_id"), literal("transactions")
                                ),
                            ),
                        ),
                        and_cond(
                            f.greaterOrEquals(
                                column("timestamp"),
                                literal(datetime(2023, 11, 23, 18, 30)),
                            ),
                            f.less(
                                column("timestamp"),
                                literal(datetime(2023, 11, 23, 22, 30)),
                            ),
                        ),
                    ),
                ),
                groupby=[
                    f.toStartOfInterval(
                        column("timestamp"),
                        f.toIntervalSecond(literal(60)),
                        literal("Universal"),
                        alias="time",
                    )
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
            None,
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
                    f.plus(
                        f.divide(
                            f.sumIf(
                                column("value", None, "_snuba_value"),
                                f.equals(
                                    column("metric_id", None, "_snuba_metric_id"),
                                    literal(123456),
                                ),
                            ),
                            f.sumIf(
                                column("value", None, "_snuba_value"),
                                f.equals(
                                    column("metric_id", None, "_snuba_metric_id"),
                                    literal(123456),
                                ),
                            ),
                        ),
                        literal(100.0),
                        alias="_snuba_aggregate_value",
                    ),
                ),
                SelectedExpression(
                    "time",
                    f.toStartOfInterval(
                        column("timestamp", None, "_snuba_timestamp"),
                        f.toIntervalSecond(literal(60)),
                        literal("Universal"),
                        alias="_snuba_time",
                    ),
                ),
            ],
            array_join=None,
            condition=and_cond(
                and_cond(
                    or_cond(
                        f.equals(
                            column("metric_id", None, "_snuba_metric_id"),
                            literal(123456),
                        ),
                        f.equals(
                            column("metric_id", None, "_snuba_metric_id"),
                            literal(123456),
                        ),
                    ),
                    and_cond(
                        f.equals(
                            column("granularity", None, "_snuba_granularity"),
                            literal(60),
                        ),
                        in_cond(
                            column("project_id", None, "_snuba_project_id"),
                            f.tuple(literal(11)),
                        ),
                    ),
                ),
                and_cond(
                    and_cond(
                        in_cond(
                            column("org_id", None, "_snuba_org_id"), f.tuple(literal(1))
                        ),
                        f.equals(
                            column("use_case_id", None, "_snuba_use_case_id"),
                            literal("transactions"),
                        ),
                    ),
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
                ),
            ),
            groupby=[
                f.toStartOfInterval(
                    column("timestamp", None, "_snuba_timestamp"),
                    f.toIntervalSecond(literal(60)),
                    literal("Universal"),
                    alias="_snuba_time",
                )
            ],
            having=None,
            order_by=[
                OrderBy(
                    OrderByDirection.ASC,
                    f.toStartOfInterval(
                        column("timestamp", None, "_snuba_timestamp"),
                        f.toIntervalSecond(literal(60)),
                        literal("Universal"),
                        alias="_snuba_time",
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
            Query(
                from_clause=Entity(
                    EntityKey.GENERIC_METRICS_DISTRIBUTIONS,
                    get_entity(
                        EntityKey.GENERIC_METRICS_DISTRIBUTIONS
                    ).get_data_model(),
                ),
                selected_columns=[
                    SelectedExpression(
                        "aggregate_value",
                        f.divide(
                            f.apdexIf(
                                f.sum(column("value")),
                                f.equals(column("metric_id"), literal(123456)),
                            ),
                            f.maxIf(
                                column("value"),
                                f.equals(column("metric_id"), literal(123456)),
                            ),
                            alias="aggregate_value",
                        ),
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
                    f.equals(column("granularity"), literal(60)),
                    and_cond(
                        and_cond(
                            in_cond(column("project_id"), f.tuple(literal(11))),
                            and_cond(
                                in_cond(column("org_id"), f.tuple(literal(1))),
                                f.equals(
                                    column("use_case_id"), literal("transactions")
                                ),
                            ),
                        ),
                        and_cond(
                            f.greaterOrEquals(
                                column("timestamp"),
                                literal(datetime(2023, 11, 23, 18, 30)),
                            ),
                            f.less(
                                column("timestamp"),
                                literal(datetime(2023, 11, 23, 22, 30)),
                            ),
                        ),
                    ),
                ),
                groupby=[
                    f.toStartOfInterval(
                        column("timestamp"),
                        f.toIntervalSecond(literal(60)),
                        literal("Universal"),
                        alias="time",
                    )
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
            None,
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
                    f.divide(
                        f.apdexIf(
                            f.sum(column("value", None, "_snuba_value")),
                            f.equals(
                                column("metric_id", None, "_snuba_metric_id"),
                                literal(123456),
                            ),
                        ),
                        f.maxIf(
                            column("value", None, "_snuba_value"),
                            f.equals(
                                column("metric_id", None, "_snuba_metric_id"),
                                literal(123456),
                            ),
                        ),
                        alias="_snuba_aggregate_value",
                    ),
                ),
                SelectedExpression(
                    "time",
                    f.toStartOfInterval(
                        column("timestamp", None, "_snuba_timestamp"),
                        f.toIntervalSecond(literal(60)),
                        literal("Universal"),
                        alias="_snuba_time",
                    ),
                ),
            ],
            array_join=None,
            condition=and_cond(
                and_cond(
                    or_cond(
                        f.equals(
                            column("metric_id", None, "_snuba_metric_id"),
                            literal(123456),
                        ),
                        f.equals(
                            column("metric_id", None, "_snuba_metric_id"),
                            literal(123456),
                        ),
                    ),
                    and_cond(
                        f.equals(
                            column("granularity", None, "_snuba_granularity"),
                            literal(60),
                        ),
                        in_cond(
                            column("project_id", None, "_snuba_project_id"),
                            f.tuple(literal(11)),
                        ),
                    ),
                ),
                and_cond(
                    and_cond(
                        in_cond(
                            column("org_id", None, "_snuba_org_id"), f.tuple(literal(1))
                        ),
                        f.equals(
                            column("use_case_id", None, "_snuba_use_case_id"),
                            literal("transactions"),
                        ),
                    ),
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
                ),
            ),
            groupby=[
                f.toStartOfInterval(
                    column("timestamp", None, "_snuba_timestamp"),
                    f.toIntervalSecond(literal(60)),
                    literal("Universal"),
                    alias="_snuba_time",
                )
            ],
            having=None,
            order_by=[
                OrderBy(
                    OrderByDirection.ASC,
                    f.toStartOfInterval(
                        column("timestamp", None, "_snuba_timestamp"),
                        f.toIntervalSecond(literal(60)),
                        literal("Universal"),
                        alias="_snuba_time",
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
            Query(
                from_clause=Entity(
                    EntityKey.GENERIC_METRICS_DISTRIBUTIONS,
                    get_entity(
                        EntityKey.GENERIC_METRICS_DISTRIBUTIONS
                    ).get_data_model(),
                ),
                selected_columns=[
                    SelectedExpression(
                        "aggregate_value",
                        f.sum(column("value"), alias="aggregate_value"),
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
                    f.equals(column("granularity"), literal(60)),
                    and_cond(
                        and_cond(
                            in_cond(column("project_id"), f.tuple(literal(11))),
                            and_cond(
                                in_cond(column("org_id"), f.tuple(literal(1))),
                                f.equals(
                                    column("use_case_id"), literal("transactions")
                                ),
                            ),
                        ),
                        and_cond(
                            and_cond(
                                f.greaterOrEquals(
                                    column("timestamp"),
                                    literal(datetime(2023, 11, 23, 18, 30)),
                                ),
                                f.less(
                                    column("timestamp"),
                                    literal(datetime(2023, 11, 23, 22, 30)),
                                ),
                            ),
                            and_cond(
                                f.equals(column("metric_id"), literal(123456)),
                                in_cond(
                                    column("tags_raw[888]"),
                                    f.tuple(literal("dist1"), literal("dist2")),
                                ),
                            ),
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
            None,
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
                    f.sum(
                        column("value", None, "_snuba_value"),
                        alias="_snuba_aggregate_value",
                    ),
                ),
                SelectedExpression("transaction", tags_raw["111111"]),
                SelectedExpression("status_code", tags_raw["222222"]),
                SelectedExpression(
                    "time",
                    f.toStartOfInterval(
                        column("timestamp", None, "_snuba_timestamp"),
                        f.toIntervalSecond(literal(60)),
                        literal("Universal"),
                        alias="_snuba_time",
                    ),
                ),
            ],
            array_join=None,
            condition=and_cond(
                and_cond(
                    and_cond(
                        f.equals(
                            column("granularity", None, "_snuba_granularity"),
                            literal(60),
                        ),
                        in_cond(
                            column("project_id", None, "_snuba_project_id"),
                            f.tuple(literal(11)),
                        ),
                    ),
                    and_cond(
                        in_cond(
                            column("org_id", None, "_snuba_org_id"), f.tuple(literal(1))
                        ),
                        f.equals(
                            column("use_case_id", None, "_snuba_use_case_id"),
                            literal("transactions"),
                        ),
                    ),
                ),
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
                        f.equals(
                            column("metric_id", None, "_snuba_metric_id"),
                            literal(123456),
                        ),
                        in_cond(
                            tags_raw["888"], f.tuple(literal("dist1"), literal("dist2"))
                        ),
                    ),
                ),
            ),
            groupby=[
                tags_raw["111111"],
                tags_raw["222222"],
                f.toStartOfInterval(
                    column("timestamp", None, "_snuba_timestamp"),
                    f.toIntervalSecond(literal(60)),
                    literal("Universal"),
                    alias="_snuba_time",
                ),
            ],
            having=None,
            order_by=[
                OrderBy(
                    OrderByDirection.ASC,
                    f.toStartOfInterval(
                        column("timestamp", None, "_snuba_timestamp"),
                        f.toIntervalSecond(literal(60)),
                        literal("Universal"),
                        alias="_snuba_time",
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
            Query(
                from_clause=Entity(
                    EntityKey.GENERIC_METRICS_DISTRIBUTIONS,
                    get_entity(
                        EntityKey.GENERIC_METRICS_DISTRIBUTIONS
                    ).get_data_model(),
                ),
                selected_columns=[
                    SelectedExpression(
                        "aggregate_value",
                        f.sum(column("value"), alias="aggregate_value"),
                    )
                ],
                array_join=None,
                condition=and_cond(
                    f.equals(column("granularity"), literal(60)),
                    and_cond(
                        and_cond(
                            in_cond(column("project_id"), f.tuple(literal(1))),
                            and_cond(
                                in_cond(column("org_id"), f.tuple(literal(1))),
                                f.equals(
                                    column("use_case_id"), literal("transactions")
                                ),
                            ),
                        ),
                        and_cond(
                            and_cond(
                                f.greaterOrEquals(
                                    column("timestamp"),
                                    literal(datetime(2021, 1, 1, 0, 0)),
                                ),
                                f.less(
                                    column("timestamp"),
                                    literal(datetime(2021, 1, 2, 0, 0)),
                                ),
                            ),
                            and_cond(
                                f.equals(column("metric_id"), literal(123456)),
                                in_cond(
                                    column("tags_raw[888]"),
                                    f.tuple(literal("dist1"), literal("dist2")),
                                ),
                            ),
                        ),
                    ),
                ),
                groupby=None,
                having=None,
                order_by=[OrderBy(OrderByDirection.ASC, column("aggregate_value"))],
                limitby=None,
                limit=1000,
                offset=0,
                totals=False,
                granularity=None,
            ),
            None,
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
                    f.sum(
                        column("value", None, "_snuba_value"),
                        alias="_snuba_aggregate_value",
                    ),
                )
            ],
            array_join=None,
            condition=and_cond(
                and_cond(
                    and_cond(
                        f.equals(
                            column("granularity", None, "_snuba_granularity"),
                            literal(60),
                        ),
                        in_cond(
                            column("project_id", None, "_snuba_project_id"),
                            f.tuple(literal(1)),
                        ),
                    ),
                    and_cond(
                        in_cond(
                            column("org_id", None, "_snuba_org_id"), f.tuple(literal(1))
                        ),
                        f.equals(
                            column("use_case_id", None, "_snuba_use_case_id"),
                            literal("transactions"),
                        ),
                    ),
                ),
                and_cond(
                    and_cond(
                        f.greaterOrEquals(
                            column("timestamp", None, "_snuba_timestamp"),
                            literal(datetime(2021, 1, 1, 0, 0)),
                        ),
                        f.less(
                            column("timestamp", None, "_snuba_timestamp"),
                            literal(datetime(2021, 1, 2, 0, 0)),
                        ),
                    ),
                    and_cond(
                        f.equals(
                            column("metric_id", None, "_snuba_metric_id"),
                            literal(123456),
                        ),
                        in_cond(
                            tags_raw["888"], f.tuple(literal("dist1"), literal("dist2"))
                        ),
                    ),
                ),
            ),
            groupby=None,
            having=None,
            order_by=[
                OrderBy(
                    OrderByDirection.ASC,
                    f.sum(
                        column("value", None, "_snuba_value"),
                        alias="_snuba_aggregate_value",
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
            Query(
                from_clause=Entity(
                    EntityKey.GENERIC_METRICS_DISTRIBUTIONS,
                    get_entity(
                        EntityKey.GENERIC_METRICS_DISTRIBUTIONS
                    ).get_data_model(),
                ),
                selected_columns=[
                    SelectedExpression(
                        "aggregate_value",
                        f.sum(column("value"), alias="aggregate_value"),
                    )
                ],
                array_join=None,
                condition=and_cond(
                    f.equals(column("granularity"), literal(60)),
                    and_cond(
                        and_cond(
                            in_cond(column("project_id"), f.tuple(literal(1))),
                            and_cond(
                                in_cond(column("org_id"), f.tuple(literal(1))),
                                f.equals(
                                    column("use_case_id"), literal("transactions")
                                ),
                            ),
                        ),
                        and_cond(
                            and_cond(
                                f.greaterOrEquals(
                                    column("timestamp"),
                                    literal(datetime(2021, 1, 1, 0, 0)),
                                ),
                                f.less(
                                    column("timestamp"),
                                    literal(datetime(2021, 1, 2, 0, 0)),
                                ),
                            ),
                            f.equals(column("metric_id"), literal(123456)),
                        ),
                    ),
                ),
                groupby=None,
                having=None,
                order_by=[OrderBy(OrderByDirection.ASC, column("aggregate_value"))],
                limitby=None,
                limit=1000,
                offset=0,
                totals=False,
                granularity=None,
            ),
            None,
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
                    f.sum(
                        column("value", None, "_snuba_value"),
                        alias="_snuba_aggregate_value",
                    ),
                )
            ],
            array_join=None,
            condition=and_cond(
                and_cond(
                    f.equals(
                        column("granularity", None, "_snuba_granularity"), literal(60)
                    ),
                    and_cond(
                        in_cond(
                            column("project_id", None, "_snuba_project_id"),
                            f.tuple(literal(1)),
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
                        f.greaterOrEquals(
                            column("timestamp", None, "_snuba_timestamp"),
                            literal(datetime(2021, 1, 1, 0, 0)),
                        ),
                    ),
                    and_cond(
                        f.less(
                            column("timestamp", None, "_snuba_timestamp"),
                            literal(datetime(2021, 1, 2, 0, 0)),
                        ),
                        f.equals(
                            column("metric_id", None, "_snuba_metric_id"),
                            literal(123456),
                        ),
                    ),
                ),
            ),
            groupby=None,
            having=None,
            order_by=[
                OrderBy(
                    OrderByDirection.ASC,
                    f.sum(
                        column("value", None, "_snuba_value"),
                        alias="_snuba_aggregate_value",
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
                ],
                array_join=None,
                condition=and_cond(
                    f.equals(column("granularity"), literal(3600)),
                    and_cond(
                        and_cond(
                            in_cond(column("project_id"), f.tuple(literal(1))),
                            and_cond(
                                in_cond(column("org_id"), f.tuple(literal(1))),
                                f.equals(
                                    column("use_case_id"), literal("transactions")
                                ),
                            ),
                        ),
                        and_cond(
                            and_cond(
                                f.greaterOrEquals(
                                    column("timestamp"),
                                    literal(datetime(2021, 1, 1, 1, 36)),
                                ),
                                f.less(
                                    column("timestamp"),
                                    literal(datetime(2021, 1, 5, 4, 15)),
                                ),
                            ),
                            and_cond(
                                f.equals(column("metric_id"), literal(567890)),
                                and_cond(
                                    f.notIn(
                                        column("tags_raw[888888]"),
                                        f.tuple(literal("dist1"), literal("dist2")),
                                    ),
                                    f.equals(
                                        column("tags_raw[777777]"), literal("bar")
                                    ),
                                ),
                            ),
                        ),
                    ),
                ),
                groupby=[column("tags_raw[111111]", None, "transaction")],
                having=None,
                order_by=None,
                limitby=None,
                limit=100,
                offset=3,
                totals=False,
                granularity=None,
            ),
            None,
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
                        "_snuba_aggregate_value",
                        f.quantiles(literal(0.5), literal(0.75)),
                        (column("value", None, "_snuba_value"),),
                    ),
                ),
                SelectedExpression("transaction", tags_raw["111111"]),
            ],
            array_join=None,
            condition=and_cond(
                f.equals(
                    column("granularity", None, "_snuba_granularity"), literal(3600)
                ),
                and_cond(
                    and_cond(
                        and_cond(
                            in_cond(
                                column("project_id", None, "_snuba_project_id"),
                                f.tuple(literal(1)),
                            ),
                            in_cond(
                                column("org_id", None, "_snuba_org_id"),
                                f.tuple(literal(1)),
                            ),
                        ),
                        and_cond(
                            f.equals(
                                column("use_case_id", None, "_snuba_use_case_id"),
                                literal("transactions"),
                            ),
                            f.greaterOrEquals(
                                column("timestamp", None, "_snuba_timestamp"),
                                literal(datetime(2021, 1, 1, 1, 36)),
                            ),
                        ),
                    ),
                    and_cond(
                        and_cond(
                            f.less(
                                column("timestamp", None, "_snuba_timestamp"),
                                literal(datetime(2021, 1, 5, 4, 15)),
                            ),
                            f.equals(
                                column("metric_id", None, "_snuba_metric_id"),
                                literal(567890),
                            ),
                        ),
                        and_cond(
                            f.notIn(
                                tags_raw["888888"],
                                f.tuple(literal("dist1"), literal("dist2")),
                            ),
                            f.equals(tags_raw["777777"], literal("bar")),
                        ),
                    ),
                ),
            ),
            groupby=[tags_raw["111111"]],
            having=None,
            order_by=None,
            limitby=None,
            limit=100,
            offset=3,
            totals=False,
            granularity=None,
        ),
    ),
    pytest.param(
        (
            Query(
                from_clause=Entity(
                    EntityKey.GENERIC_METRICS_DISTRIBUTIONS,
                    get_entity(
                        EntityKey.GENERIC_METRICS_DISTRIBUTIONS
                    ).get_data_model(),
                ),
                selected_columns=[
                    SelectedExpression(
                        "aggregate_value",
                        CurriedFunctionCall(
                            "aggregate_value",
                            f.quantiles(literal(0.5)),
                            (column("value"),),
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
                    f.equals(column("granularity"), literal(60)),
                    and_cond(
                        and_cond(
                            in_cond(column("project_id"), f.tuple(literal(11))),
                            and_cond(
                                in_cond(column("org_id"), f.tuple(literal(1))),
                                f.equals(
                                    column("use_case_id"), literal("transactions")
                                ),
                            ),
                        ),
                        and_cond(
                            and_cond(
                                f.greaterOrEquals(
                                    column("timestamp"),
                                    literal(datetime(2023, 11, 23, 18, 30)),
                                ),
                                f.less(
                                    column("timestamp"),
                                    literal(datetime(2023, 11, 23, 22, 30)),
                                ),
                            ),
                            and_cond(
                                f.equals(column("metric_id"), literal(123456)),
                                in_cond(
                                    column("tags_raw[888]"),
                                    f.tuple(literal("dist1"), literal("dist2")),
                                ),
                            ),
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
            None,
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
                    f.arrayElement(
                        CurriedFunctionCall(
                            None,
                            f.quantiles(literal(0.5)),
                            (column("value", None, "_snuba_value"),),
                        ),
                        literal(1),
                        alias="_snuba_aggregate_value",
                    ),
                ),
                SelectedExpression("transaction", tags_raw["111111"]),
                SelectedExpression("status_code", tags_raw["222222"]),
                SelectedExpression(
                    "time",
                    f.toStartOfInterval(
                        column("timestamp", None, "_snuba_timestamp"),
                        f.toIntervalSecond(literal(60)),
                        literal("Universal"),
                        alias="_snuba_time",
                    ),
                ),
            ],
            array_join=None,
            condition=and_cond(
                and_cond(
                    and_cond(
                        f.equals(
                            column("granularity", None, "_snuba_granularity"),
                            literal(60),
                        ),
                        in_cond(
                            column("project_id", None, "_snuba_project_id"),
                            f.tuple(literal(11)),
                        ),
                    ),
                    and_cond(
                        in_cond(
                            column("org_id", None, "_snuba_org_id"), f.tuple(literal(1))
                        ),
                        f.equals(
                            column("use_case_id", None, "_snuba_use_case_id"),
                            literal("transactions"),
                        ),
                    ),
                ),
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
                        f.equals(
                            column("metric_id", None, "_snuba_metric_id"),
                            literal(123456),
                        ),
                        in_cond(
                            tags_raw["888"], f.tuple(literal("dist1"), literal("dist2"))
                        ),
                    ),
                ),
            ),
            groupby=[
                tags_raw["111111"],
                tags_raw["222222"],
                f.toStartOfInterval(
                    column("timestamp", None, "_snuba_timestamp"),
                    f.toIntervalSecond(literal(60)),
                    literal("Universal"),
                    alias="_snuba_time",
                ),
            ],
            having=None,
            order_by=[
                OrderBy(
                    OrderByDirection.ASC,
                    f.toStartOfInterval(
                        column("timestamp", None, "_snuba_timestamp"),
                        f.toIntervalSecond(literal(60)),
                        literal("Universal"),
                        alias="_snuba_time",
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
            Query(
                from_clause=Entity(
                    EntityKey.METRICS_DISTRIBUTIONS,
                    get_entity(EntityKey.METRICS_DISTRIBUTIONS).get_data_model(),
                ),
                selected_columns=[
                    SelectedExpression(
                        "aggregate_value",
                        f.sum(column("value"), alias="aggregate_value"),
                    ),
                    SelectedExpression("release", column("tags[111]", None, "release")),
                ],
                array_join=None,
                condition=and_cond(
                    f.equals(column("granularity"), literal(60)),
                    and_cond(
                        and_cond(
                            in_cond(column("project_id"), f.tuple(literal(1))),
                            and_cond(
                                in_cond(column("org_id"), f.tuple(literal(1))),
                                f.equals(column("use_case_id"), literal("sessions")),
                            ),
                        ),
                        and_cond(
                            and_cond(
                                f.greaterOrEquals(
                                    column("timestamp"),
                                    literal(datetime(2021, 1, 1, 0, 0)),
                                ),
                                f.less(
                                    column("timestamp"),
                                    literal(datetime(2021, 1, 2, 0, 0)),
                                ),
                            ),
                            and_cond(
                                f.equals(column("metric_id"), literal(123456)),
                                in_cond(
                                    column("tags[111]"),
                                    f.tuple(literal(222), literal(333)),
                                ),
                            ),
                        ),
                    ),
                ),
                groupby=[column("tags[111]", None, "release")],
                having=None,
                order_by=[OrderBy(OrderByDirection.ASC, column("aggregate_value"))],
                limitby=None,
                limit=1000,
                offset=0,
                totals=False,
                granularity=None,
            ),
            None,
            None,
        ),
        Query(
            from_clause=Entity(
                EntityKey.METRICS_DISTRIBUTIONS,
                get_entity(EntityKey.METRICS_DISTRIBUTIONS).get_data_model(),
            ),
            selected_columns=[
                SelectedExpression(
                    "aggregate_value",
                    f.sum(
                        column("value", None, "_snuba_value"),
                        alias="_snuba_aggregate_value",
                    ),
                ),
                SelectedExpression("release", tags["111"]),
            ],
            array_join=None,
            condition=and_cond(
                and_cond(
                    and_cond(
                        f.equals(
                            column("granularity", None, "_snuba_granularity"),
                            literal(60),
                        ),
                        in_cond(
                            column("project_id", None, "_snuba_project_id"),
                            f.tuple(literal(1)),
                        ),
                    ),
                    and_cond(
                        in_cond(
                            column("org_id", None, "_snuba_org_id"), f.tuple(literal(1))
                        ),
                        f.equals(
                            column("use_case_id", None, "_snuba_use_case_id"),
                            literal("sessions"),
                        ),
                    ),
                ),
                and_cond(
                    and_cond(
                        f.greaterOrEquals(
                            column("timestamp", None, "_snuba_timestamp"),
                            literal(datetime(2021, 1, 1, 0, 0)),
                        ),
                        f.less(
                            column("timestamp", None, "_snuba_timestamp"),
                            literal(datetime(2021, 1, 2, 0, 0)),
                        ),
                    ),
                    and_cond(
                        f.equals(
                            column("metric_id", None, "_snuba_metric_id"),
                            literal(123456),
                        ),
                        in_cond(tags["111"], f.tuple(literal(222), literal(333))),
                    ),
                ),
            ),
            groupby=[tags["111"]],
            having=None,
            order_by=[
                OrderBy(
                    OrderByDirection.ASC,
                    f.sum(
                        column("value", None, "_snuba_value"),
                        alias="_snuba_aggregate_value",
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
            Query(
                from_clause=Entity(
                    EntityKey.GENERIC_METRICS_DISTRIBUTIONS,
                    get_entity(
                        EntityKey.GENERIC_METRICS_DISTRIBUTIONS
                    ).get_data_model(),
                ),
                selected_columns=[
                    SelectedExpression(
                        "aggregate_value",
                        f.max(column("value"), alias="aggregate_value"),
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
                    f.equals(column("granularity"), literal(60)),
                    and_cond(
                        and_cond(
                            in_cond(column("project_id"), f.tuple(literal(1))),
                            and_cond(
                                in_cond(column("org_id"), f.tuple(literal(1))),
                                f.equals(
                                    column("use_case_id"), literal("transactions")
                                ),
                            ),
                        ),
                        and_cond(
                            and_cond(
                                f.greaterOrEquals(
                                    column("timestamp"),
                                    literal(datetime(2024, 1, 7, 13, 35)),
                                ),
                                f.less(
                                    column("timestamp"),
                                    literal(datetime(2024, 1, 8, 13, 40)),
                                ),
                            ),
                            and_cond(
                                f.equals(column("metric_id"), literal(123456)),
                                f.equals(
                                    column("tags_raw[111213]"),
                                    literal(
                                        " !\"#$%&'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\\\]^_`abcdefghijklmnopqrstuvwxyz{|}~"
                                    ),
                                ),
                            ),
                        ),
                    ),
                ),
                groupby=[
                    column("tags_raw[141516]", None, "transaction"),
                    f.toStartOfInterval(
                        column("timestamp"),
                        f.toIntervalSecond(literal(300)),
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
                            f.toIntervalSecond(literal(300)),
                            literal("Universal"),
                            alias="time",
                        ),
                    )
                ],
                limitby=None,
                limit=10000,
                offset=0,
                totals=False,
                granularity=None,
            ),
            None,
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
                    f.max(
                        column("value", None, "_snuba_value"),
                        alias="_snuba_aggregate_value",
                    ),
                ),
                SelectedExpression("transaction", tags_raw["141516"]),
                SelectedExpression(
                    "time",
                    f.toStartOfInterval(
                        column("timestamp", None, "_snuba_timestamp"),
                        f.toIntervalSecond(literal(300)),
                        literal("Universal"),
                        alias="_snuba_time",
                    ),
                ),
            ],
            array_join=None,
            condition=and_cond(
                and_cond(
                    and_cond(
                        f.equals(
                            column("granularity", None, "_snuba_granularity"),
                            literal(60),
                        ),
                        in_cond(
                            column("project_id", None, "_snuba_project_id"),
                            f.tuple(literal(1)),
                        ),
                    ),
                    and_cond(
                        in_cond(
                            column("org_id", None, "_snuba_org_id"), f.tuple(literal(1))
                        ),
                        f.equals(
                            column("use_case_id", None, "_snuba_use_case_id"),
                            literal("transactions"),
                        ),
                    ),
                ),
                and_cond(
                    and_cond(
                        f.greaterOrEquals(
                            column("timestamp", None, "_snuba_timestamp"),
                            literal(datetime(2024, 1, 7, 13, 35)),
                        ),
                        f.less(
                            column("timestamp", None, "_snuba_timestamp"),
                            literal(datetime(2024, 1, 8, 13, 40)),
                        ),
                    ),
                    and_cond(
                        f.equals(
                            column("metric_id", None, "_snuba_metric_id"),
                            literal(123456),
                        ),
                        f.equals(
                            tags_raw["111213"],
                            literal(
                                " !\"#$%&'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\\\]^_`abcdefghijklmnopqrstuvwxyz{|}~"
                            ),
                        ),
                    ),
                ),
            ),
            groupby=[
                tags_raw["141516"],
                f.toStartOfInterval(
                    column("timestamp", None, "_snuba_timestamp"),
                    f.toIntervalSecond(literal(300)),
                    literal("Universal"),
                    alias="_snuba_time",
                ),
            ],
            having=None,
            order_by=[
                OrderBy(
                    OrderByDirection.ASC,
                    f.toStartOfInterval(
                        column("timestamp", None, "_snuba_timestamp"),
                        f.toIntervalSecond(literal(300)),
                        literal("Universal"),
                        alias="_snuba_time",
                    ),
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
            Query(
                from_clause=Entity(
                    EntityKey.GENERIC_METRICS_DISTRIBUTIONS,
                    get_entity(
                        EntityKey.GENERIC_METRICS_DISTRIBUTIONS
                    ).get_data_model(),
                ),
                selected_columns=[
                    SelectedExpression(
                        "aggregate_value",
                        f.apdex(
                            f.sum(column("value")),
                            literal(500.0),
                            alias="aggregate_value",
                        ),
                    )
                ],
                array_join=None,
                condition=and_cond(
                    f.equals(column("granularity"), literal(60)),
                    and_cond(
                        and_cond(
                            in_cond(column("project_id"), f.tuple(literal(1))),
                            and_cond(
                                in_cond(column("org_id"), f.tuple(literal(1))),
                                f.equals(
                                    column("use_case_id"), literal("transactions")
                                ),
                            ),
                        ),
                        and_cond(
                            and_cond(
                                f.greaterOrEquals(
                                    column("timestamp"),
                                    literal(datetime(2021, 1, 1, 0, 0)),
                                ),
                                f.less(
                                    column("timestamp"),
                                    literal(datetime(2021, 1, 2, 0, 0)),
                                ),
                            ),
                            and_cond(
                                f.equals(column("metric_id"), literal(123456)),
                                in_cond(
                                    column("tags_raw[888]"),
                                    f.tuple(literal("dist1"), literal("dist2")),
                                ),
                            ),
                        ),
                    ),
                ),
                groupby=None,
                having=None,
                order_by=[OrderBy(OrderByDirection.ASC, column("aggregate_value"))],
                limitby=None,
                limit=1000,
                offset=0,
                totals=False,
                granularity=None,
            ),
            None,
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
                        f.sum(column("value", None, "_snuba_value")),
                        literal(500.0),
                        alias="_snuba_aggregate_value",
                    ),
                )
            ],
            array_join=None,
            condition=and_cond(
                and_cond(
                    and_cond(
                        f.equals(
                            column("granularity", None, "_snuba_granularity"),
                            literal(60),
                        ),
                        in_cond(
                            column("project_id", None, "_snuba_project_id"),
                            f.tuple(literal(1)),
                        ),
                    ),
                    and_cond(
                        in_cond(
                            column("org_id", None, "_snuba_org_id"), f.tuple(literal(1))
                        ),
                        f.equals(
                            column("use_case_id", None, "_snuba_use_case_id"),
                            literal("transactions"),
                        ),
                    ),
                ),
                and_cond(
                    and_cond(
                        f.greaterOrEquals(
                            column("timestamp", None, "_snuba_timestamp"),
                            literal(datetime(2021, 1, 1, 0, 0)),
                        ),
                        f.less(
                            column("timestamp", None, "_snuba_timestamp"),
                            literal(datetime(2021, 1, 2, 0, 0)),
                        ),
                    ),
                    and_cond(
                        f.equals(
                            column("metric_id", None, "_snuba_metric_id"),
                            literal(123456),
                        ),
                        in_cond(
                            tags_raw["888"], f.tuple(literal("dist1"), literal("dist2"))
                        ),
                    ),
                ),
            ),
            groupby=None,
            having=None,
            order_by=[
                OrderBy(
                    OrderByDirection.ASC,
                    f.apdex(
                        f.sum(column("value", None, "_snuba_value")),
                        literal(500.0),
                        alias="_snuba_aggregate_value",
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
                    )
                ],
                array_join=None,
                condition=and_cond(
                    f.equals(column("granularity"), literal(3600)),
                    and_cond(
                        and_cond(
                            in_cond(column("project_id"), f.tuple(literal(1))),
                            and_cond(
                                in_cond(column("org_id"), f.tuple(literal(1))),
                                f.equals(
                                    column("use_case_id"), literal("transactions")
                                ),
                            ),
                        ),
                        and_cond(
                            and_cond(
                                f.greaterOrEquals(
                                    column("timestamp"),
                                    literal(datetime(2021, 1, 1, 1, 36)),
                                ),
                                f.less(
                                    column("timestamp"),
                                    literal(datetime(2021, 1, 5, 4, 15)),
                                ),
                            ),
                            f.equals(column("metric_id"), literal(567890)),
                        ),
                    ),
                ),
                groupby=None,
                having=None,
                order_by=None,
                limitby=None,
                limit=100,
                offset=3,
                totals=False,
                granularity=None,
            ),
            None,
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
                            f.sum(
                                column("value", None, "_snuba_value"),
                                alias="_snuba_aggregate_value",
                            ),
                            literal(300.0),
                        ),
                    ),
                )
            ],
            array_join=None,
            condition=and_cond(
                and_cond(
                    f.equals(
                        column("granularity", None, "_snuba_granularity"), literal(3600)
                    ),
                    and_cond(
                        in_cond(
                            column("project_id", None, "_snuba_project_id"),
                            f.tuple(literal(1)),
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
                        f.greaterOrEquals(
                            column("timestamp", None, "_snuba_timestamp"),
                            literal(datetime(2021, 1, 1, 1, 36)),
                        ),
                    ),
                    and_cond(
                        f.less(
                            column("timestamp", None, "_snuba_timestamp"),
                            literal(datetime(2021, 1, 5, 4, 15)),
                        ),
                        f.equals(
                            column("metric_id", None, "_snuba_metric_id"),
                            literal(567890),
                        ),
                    ),
                ),
            ),
            groupby=None,
            having=None,
            order_by=None,
            limitby=None,
            limit=100,
            offset=3,
            totals=False,
            granularity=None,
        ),
    ),
    pytest.param(
        (
            Query(
                from_clause=Entity(
                    EntityKey.GENERIC_METRICS_DISTRIBUTIONS,
                    get_entity(
                        EntityKey.GENERIC_METRICS_DISTRIBUTIONS
                    ).get_data_model(),
                ),
                selected_columns=[
                    SelectedExpression(
                        "aggregate_value",
                        f.avg(column("value"), alias="aggregate_value"),
                    )
                ],
                array_join=None,
                condition=and_cond(
                    f.equals(column("granularity"), literal(60)),
                    and_cond(
                        and_cond(
                            in_cond(column("project_id"), f.tuple(literal(1))),
                            and_cond(
                                in_cond(column("org_id"), f.tuple(literal(1))),
                                f.equals(column("use_case_id"), literal("custom")),
                            ),
                        ),
                        and_cond(
                            and_cond(
                                f.greaterOrEquals(
                                    column("timestamp"),
                                    literal(datetime(2021, 1, 1, 0, 0)),
                                ),
                                f.less(
                                    column("timestamp"),
                                    literal(datetime(2021, 1, 2, 0, 0)),
                                ),
                            ),
                            and_cond(
                                f.equals(column("metric_id"), literal(111111)),
                                and_cond(
                                    f.equals(
                                        column("tags_raw[222222]"),
                                        literal("transaction"),
                                    ),
                                    f.equals(
                                        column("tags_raw[333333]"),
                                        literal(
                                            "sentry.tasks.store.save_event_transaction"
                                        ),
                                    ),
                                ),
                            ),
                        ),
                    ),
                ),
                groupby=None,
                having=None,
                order_by=None,
                limitby=None,
                limit=1000,
                offset=0,
                totals=False,
                granularity=None,
            ),
            None,
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
                    f.avg(
                        column("value", None, "_snuba_value"),
                        alias="_snuba_aggregate_value",
                    ),
                )
            ],
            array_join=None,
            condition=and_cond(
                f.equals(
                    column("granularity", None, "_snuba_granularity"), literal(60)
                ),
                and_cond(
                    and_cond(
                        and_cond(
                            in_cond(
                                column("project_id", None, "_snuba_project_id"),
                                f.tuple(literal(1)),
                            ),
                            in_cond(
                                column("org_id", None, "_snuba_org_id"),
                                f.tuple(literal(1)),
                            ),
                        ),
                        and_cond(
                            f.equals(
                                column("use_case_id", None, "_snuba_use_case_id"),
                                literal("custom"),
                            ),
                            f.greaterOrEquals(
                                column("timestamp", None, "_snuba_timestamp"),
                                literal(datetime(2021, 1, 1, 0, 0)),
                            ),
                        ),
                    ),
                    and_cond(
                        and_cond(
                            f.less(
                                column("timestamp", None, "_snuba_timestamp"),
                                literal(datetime(2021, 1, 2, 0, 0)),
                            ),
                            f.equals(
                                column("metric_id", None, "_snuba_metric_id"),
                                literal(111111),
                            ),
                        ),
                        and_cond(
                            f.equals(tags_raw["222222"], literal("transaction")),
                            f.equals(
                                tags_raw["333333"],
                                literal("sentry.tasks.store.save_event_transaction"),
                            ),
                        ),
                    ),
                ),
            ),
            groupby=None,
            having=None,
            order_by=None,
            limitby=None,
            limit=1000,
            offset=0,
            totals=False,
            granularity=None,
        ),
    ),
    pytest.param(
        (
            Query(
                from_clause=Entity(
                    EntityKey.GENERIC_METRICS_DISTRIBUTIONS,
                    get_entity(
                        EntityKey.GENERIC_METRICS_DISTRIBUTIONS
                    ).get_data_model(),
                ),
                selected_columns=[
                    SelectedExpression(
                        "aggregate_value",
                        f.multiply(
                            f.multiply(
                                f.avgIf(
                                    column("value"),
                                    and_cond(
                                        f.equals(
                                            column("tags_raw[9223372036854776020]"),
                                            literal(
                                                "getsentry.tasks.calculate_spike_projections"
                                            ),
                                        ),
                                        f.equals(
                                            column("metric_id"),
                                            literal(9223372036854775909),
                                        ),
                                    ),
                                ),
                                literal(100.0),
                            ),
                            literal(100.0),
                            alias="aggregate_value",
                        ),
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
                    f.equals(column("granularity"), literal(60)),
                    and_cond(
                        and_cond(
                            in_cond(column("project_id"), f.tuple(literal(1))),
                            and_cond(
                                in_cond(column("org_id"), f.tuple(literal(1))),
                                f.equals(
                                    column("use_case_id"), literal("'transactions'")
                                ),
                            ),
                        ),
                        and_cond(
                            f.greaterOrEquals(
                                column("timestamp"),
                                literal(datetime(2024, 4, 8, 5, 48)),
                            ),
                            f.less(
                                column("timestamp"),
                                literal(datetime(2024, 4, 8, 6, 49)),
                            ),
                        ),
                    ),
                ),
                groupby=[
                    f.toStartOfInterval(
                        column("timestamp"),
                        f.toIntervalSecond(literal(60)),
                        literal("Universal"),
                        alias="time",
                    )
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
                limit=10000,
                offset=0,
                totals=False,
                granularity=None,
            ),
            None,
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
                    f.multiply(
                        f.multiply(
                            f.avgIf(
                                column("value", None, "_snuba_value"),
                                and_cond(
                                    f.equals(
                                        tags_raw["9223372036854776020"],
                                        literal(
                                            "getsentry.tasks.calculate_spike_projections"
                                        ),
                                    ),
                                    f.equals(
                                        column("metric_id", None, "_snuba_metric_id"),
                                        literal(9223372036854775909),
                                    ),
                                ),
                            ),
                            literal(100.0),
                        ),
                        literal(100.0),
                        alias="_snuba_aggregate_value",
                    ),
                ),
                SelectedExpression(
                    "time",
                    f.toStartOfInterval(
                        column("timestamp", None, "_snuba_timestamp"),
                        f.toIntervalSecond(literal(60)),
                        literal("Universal"),
                        alias="_snuba_time",
                    ),
                ),
            ],
            array_join=None,
            condition=and_cond(
                and_cond(
                    and_cond(
                        f.equals(
                            tags_raw["9223372036854776020"],
                            literal("getsentry.tasks.calculate_spike_projections"),
                        ),
                        f.equals(
                            column("metric_id", None, "_snuba_metric_id"),
                            literal(9223372036854775909),
                        ),
                    ),
                    and_cond(
                        f.equals(
                            column("granularity", None, "_snuba_granularity"),
                            literal(60),
                        ),
                        in_cond(
                            column("project_id", None, "_snuba_project_id"),
                            f.tuple(literal(1)),
                        ),
                    ),
                ),
                and_cond(
                    and_cond(
                        in_cond(
                            column("org_id", None, "_snuba_org_id"), f.tuple(literal(1))
                        ),
                        f.equals(
                            column("use_case_id", None, "_snuba_use_case_id"),
                            literal("'transactions'"),
                        ),
                    ),
                    and_cond(
                        f.greaterOrEquals(
                            column("timestamp", None, "_snuba_timestamp"),
                            literal(datetime(2024, 4, 8, 5, 48)),
                        ),
                        f.less(
                            column("timestamp", None, "_snuba_timestamp"),
                            literal(datetime(2024, 4, 8, 6, 49)),
                        ),
                    ),
                ),
            ),
            groupby=[
                f.toStartOfInterval(
                    column("timestamp", None, "_snuba_timestamp"),
                    f.toIntervalSecond(literal(60)),
                    literal("Universal"),
                    alias="_snuba_time",
                )
            ],
            having=None,
            order_by=[
                OrderBy(
                    OrderByDirection.ASC,
                    f.toStartOfInterval(
                        column("timestamp", None, "_snuba_timestamp"),
                        f.toIntervalSecond(literal(60)),
                        literal("Universal"),
                        alias="_snuba_time",
                    ),
                )
            ],
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
    theinput: tuple[Query, QuerySettings | None, CustomProcessors | None],
    expected: Query | CompositeQuery[Entity],
) -> None:
    timer = Timer("mql_pipeline")
    res = PostProcessAndValidateMQLQuery().execute(
        QueryPipelineResult(
            data=theinput,
            error=None,
            query_settings=HTTPQuerySettings(),
            timer=timer,
        )
    )
    assert res.data and not res.error
    eq, reason = res.data.equals(expected)
    assert eq, reason
