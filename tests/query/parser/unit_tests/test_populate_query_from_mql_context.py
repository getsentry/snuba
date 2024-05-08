from datetime import datetime
import re

import pytest

from snuba.datasets.dataset import Dataset
from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.factory import get_dataset
from snuba.query import OrderBy, OrderByDirection, SelectedExpression
from snuba.query.data_source.simple import Entity
from snuba.query.dsl import Functions as f
from snuba.query.dsl import (
    and_cond,
    column,
    divide,
    equals,
    greaterOrEquals,
    in_cond,
    less,
    literal,
    literals_tuple,
    multiply,
    plus,
)
from snuba.query.expressions import CurriedFunctionCall
from snuba.query.logical import Query
from snuba.query.mql.parser import parse_mql_query_body, populate_query_from_mql_context
from snuba.query.parser.exceptions import ParsingException

test_cases = [
    pytest.param(
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
                            column("value"),
                            and_cond(
                                f.equals(column("status_code"), literal("200")),
                                f.equals(
                                    column("metric_id"),
                                    literal("d:transactions/duration@millisecond"),
                                ),
                            ),
                        ),
                        f.sumIf(
                            column("value"),
                            f.equals(
                                column("metric_id"),
                                literal("d:transactions/duration@millisecond"),
                            ),
                        ),
                        alias="aggregate_value",
                    ),
                )
            ],
            array_join=None,
            condition=None,
            groupby=None,
            having=None,
            order_by=None,
            limitby=None,
            limit=None,
            offset=0,
            totals=False,
            granularity=None,
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
                            column("value"),
                            and_cond(
                                f.equals(column("status_code"), literal("200")),
                                f.equals(
                                    column("metric_id"),
                                    literal("d:transactions/duration@millisecond"),
                                ),
                            ),
                        ),
                        f.sumIf(
                            column("value"),
                            f.equals(
                                column("metric_id"),
                                literal("d:transactions/duration@millisecond"),
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
                in_cond(column("project_id"), f.tuple(literal(11))),
                in_cond(column("org_id"), f.tuple(literal(1))),
                f.equals(column("use_case_id"), literal("transactions")),
                f.greaterOrEquals(
                    column("timestamp"), literal(datetime(2023, 11, 23, 18, 30))
                ),
                f.less(column("timestamp"), literal(datetime(2023, 11, 23, 22, 30))),
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
    ),
    pytest.param(
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
                                column("value"),
                                and_cond(
                                    f.equals(column("status_code"), literal("200")),
                                    f.equals(
                                        column("metric_id"),
                                        literal("d:transactions/duration@millisecond"),
                                    ),
                                ),
                            ),
                            f.sumIf(
                                column("value"),
                                f.equals(
                                    column("metric_id"),
                                    literal("d:transactions/duration@millisecond"),
                                ),
                            ),
                        ),
                        alias="aggregate_value",
                    ),
                )
            ],
            array_join=None,
            condition=None,
            groupby=None,
            having=None,
            order_by=None,
            limitby=None,
            limit=None,
            offset=0,
            totals=False,
            granularity=None,
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
                                column("value"),
                                and_cond(
                                    f.equals(column("status_code"), literal("200")),
                                    f.equals(
                                        column("metric_id"),
                                        literal("d:transactions/duration@millisecond"),
                                    ),
                                ),
                            ),
                            f.sumIf(
                                column("value"),
                                f.equals(
                                    column("metric_id"),
                                    literal("d:transactions/duration@millisecond"),
                                ),
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
                in_cond(column("project_id"), f.tuple(literal(11))),
                in_cond(column("org_id"), f.tuple(literal(1))),
                f.equals(column("use_case_id"), literal("transactions")),
                f.greaterOrEquals(
                    column("timestamp"), literal(datetime(2023, 11, 23, 18, 30))
                ),
                f.less(column("timestamp"), literal(datetime(2023, 11, 23, 22, 30))),
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
    ),
    pytest.param(
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
                            column("value"),
                            and_cond(
                                f.equals(column("status_code"), literal("200")),
                                f.equals(
                                    column("metric_id"),
                                    literal("d:transactions/duration@millisecond"),
                                ),
                            ),
                        ),
                        f.sumIf(
                            column("value"),
                            f.equals(
                                column("metric_id"),
                                literal("d:transactions/duration@millisecond"),
                            ),
                        ),
                        alias="aggregate_value",
                    ),
                ),
                SelectedExpression(
                    "transaction", column("transaction", None, "transaction")
                ),
            ],
            array_join=None,
            condition=None,
            groupby=[column("transaction", None, "transaction")],
            having=None,
            order_by=None,
            limitby=None,
            limit=None,
            offset=0,
            totals=False,
            granularity=None,
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
                            column("value"),
                            and_cond(
                                f.equals(column("status_code"), literal("200")),
                                f.equals(
                                    column("metric_id"),
                                    literal("d:transactions/duration@millisecond"),
                                ),
                            ),
                        ),
                        f.sumIf(
                            column("value"),
                            f.equals(
                                column("metric_id"),
                                literal("d:transactions/duration@millisecond"),
                            ),
                        ),
                        alias="aggregate_value",
                    ),
                ),
                SelectedExpression(
                    "transaction", column("transaction", None, "transaction")
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
                in_cond(column("project_id"), f.tuple(literal(11))),
                in_cond(column("org_id"), f.tuple(literal(1))),
                f.equals(column("use_case_id"), literal("transactions")),
                f.greaterOrEquals(
                    column("timestamp"), literal(datetime(2023, 11, 23, 18, 30))
                ),
                f.less(column("timestamp"), literal(datetime(2023, 11, 23, 22, 30))),
            ),
            groupby=[
                column("transaction", None, "transaction"),
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
        Query(
            from_clause=Entity(
                EntityKey.GENERIC_METRICS_DISTRIBUTIONS,
                get_entity(EntityKey.GENERIC_METRICS_DISTRIBUTIONS).get_data_model(),
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
                                    f.equals(column("status_code"), literal("200")),
                                    f.equals(
                                        column("metric_id"),
                                        literal("d:transactions/duration@millisecond"),
                                    ),
                                ),
                            ),
                        ),
                        f.sumIf(
                            column("value"),
                            f.equals(
                                column("metric_id"),
                                literal("d:transactions/duration@millisecond"),
                            ),
                        ),
                        alias="aggregate_value",
                    ),
                ),
                SelectedExpression(
                    "transaction", column("transaction", None, "transaction")
                ),
            ],
            array_join=None,
            condition=None,
            groupby=[column("transaction", None, "transaction")],
            having=None,
            order_by=None,
            limitby=None,
            limit=None,
            offset=0,
            totals=False,
            granularity=None,
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
                        CurriedFunctionCall(
                            None,
                            f.quantilesIf(literal(0.5)),
                            (
                                column("value"),
                                and_cond(
                                    f.equals(column("status_code"), literal("200")),
                                    f.equals(
                                        column("metric_id"),
                                        literal("d:transactions/duration@millisecond"),
                                    ),
                                ),
                            ),
                        ),
                        f.sumIf(
                            column("value"),
                            f.equals(
                                column("metric_id"),
                                literal("d:transactions/duration@millisecond"),
                            ),
                        ),
                        alias="aggregate_value",
                    ),
                ),
                SelectedExpression(
                    "transaction", column("transaction", None, "transaction")
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
                in_cond(column("project_id"), f.tuple(literal(11))),
                in_cond(column("org_id"), f.tuple(literal(1))),
                f.equals(column("use_case_id"), literal("transactions")),
                f.greaterOrEquals(
                    column("timestamp"), literal(datetime(2023, 11, 23, 18, 30))
                ),
                f.less(column("timestamp"), literal(datetime(2023, 11, 23, 22, 30))),
            ),
            groupby=[
                column("transaction", None, "transaction"),
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
                            column("value"),
                            f.equals(
                                column("metric_id"),
                                literal("d:transactions/duration@millisecond"),
                            ),
                        ),
                        f.multiply(
                            f.plus(
                                f.maxIf(
                                    column("value"),
                                    f.equals(
                                        column("metric_id"),
                                        literal("d:transactions/duration@millisecond"),
                                    ),
                                ),
                                f.avgIf(
                                    column("value"),
                                    f.equals(
                                        column("metric_id"),
                                        literal("d:transactions/duration@millisecond"),
                                    ),
                                ),
                            ),
                            f.minIf(
                                column("value"),
                                f.equals(
                                    column("metric_id"),
                                    literal("d:transactions/duration@millisecond"),
                                ),
                            ),
                        ),
                        alias="aggregate_value",
                    ),
                )
            ],
            array_join=None,
            condition=None,
            groupby=None,
            having=None,
            order_by=None,
            limitby=None,
            limit=None,
            offset=0,
            totals=False,
            granularity=None,
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
                            column("value"),
                            f.equals(
                                column("metric_id"),
                                literal("d:transactions/duration@millisecond"),
                            ),
                        ),
                        f.multiply(
                            f.plus(
                                f.maxIf(
                                    column("value"),
                                    f.equals(
                                        column("metric_id"),
                                        literal("d:transactions/duration@millisecond"),
                                    ),
                                ),
                                f.avgIf(
                                    column("value"),
                                    f.equals(
                                        column("metric_id"),
                                        literal("d:transactions/duration@millisecond"),
                                    ),
                                ),
                            ),
                            f.minIf(
                                column("value"),
                                f.equals(
                                    column("metric_id"),
                                    literal("d:transactions/duration@millisecond"),
                                ),
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
                in_cond(column("project_id"), f.tuple(literal(11))),
                in_cond(column("org_id"), f.tuple(literal(1))),
                f.equals(column("use_case_id"), literal("transactions")),
                f.greaterOrEquals(
                    column("timestamp"), literal(datetime(2023, 11, 23, 18, 30))
                ),
                f.less(column("timestamp"), literal(datetime(2023, 11, 23, 22, 30))),
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
    ),
    pytest.param(
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
                            column("value"),
                            and_cond(
                                f.equals(column("status_code"), literal("200")),
                                f.equals(
                                    column("metric_id"),
                                    literal("d:transactions/duration@millisecond"),
                                ),
                            ),
                        ),
                        f.maxIf(
                            column("value"),
                            and_cond(
                                f.equals(column("status_code"), literal("200")),
                                f.equals(
                                    column("metric_id"),
                                    literal("d:transactions/duration@millisecond"),
                                ),
                            ),
                        ),
                        alias="aggregate_value",
                    ),
                )
            ],
            array_join=None,
            condition=None,
            groupby=None,
            having=None,
            order_by=None,
            limitby=None,
            limit=None,
            offset=0,
            totals=False,
            granularity=None,
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
                            column("value"),
                            and_cond(
                                f.equals(column("status_code"), literal("200")),
                                f.equals(
                                    column("metric_id"),
                                    literal("d:transactions/duration@millisecond"),
                                ),
                            ),
                        ),
                        f.maxIf(
                            column("value"),
                            and_cond(
                                f.equals(column("status_code"), literal("200")),
                                f.equals(
                                    column("metric_id"),
                                    literal("d:transactions/duration@millisecond"),
                                ),
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
                in_cond(column("project_id"), f.tuple(literal(11))),
                in_cond(column("org_id"), f.tuple(literal(1))),
                f.equals(column("use_case_id"), literal("transactions")),
                f.greaterOrEquals(
                    column("timestamp"), literal(datetime(2023, 11, 23, 18, 30))
                ),
                f.less(column("timestamp"), literal(datetime(2023, 11, 23, 22, 30))),
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
    ),
    pytest.param(
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
                            column("value"),
                            and_cond(
                                f.equals(column("status_code"), literal("200")),
                                f.equals(
                                    column("metric_id"),
                                    literal("d:transactions/duration@millisecond"),
                                ),
                            ),
                        ),
                        f.maxIf(
                            column("value"),
                            and_cond(
                                f.equals(column("status_code"), literal("200")),
                                f.equals(
                                    column("metric_id"),
                                    literal("d:transactions/duration@millisecond"),
                                ),
                            ),
                        ),
                        alias="aggregate_value",
                    ),
                ),
                SelectedExpression(
                    "transaction", column("transaction", None, "transaction")
                ),
            ],
            array_join=None,
            condition=None,
            groupby=[column("transaction", None, "transaction")],
            having=None,
            order_by=None,
            limitby=None,
            limit=None,
            offset=0,
            totals=False,
            granularity=None,
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
                            column("value"),
                            and_cond(
                                f.equals(column("status_code"), literal("200")),
                                f.equals(
                                    column("metric_id"),
                                    literal("d:transactions/duration@millisecond"),
                                ),
                            ),
                        ),
                        f.maxIf(
                            column("value"),
                            and_cond(
                                f.equals(column("status_code"), literal("200")),
                                f.equals(
                                    column("metric_id"),
                                    literal("d:transactions/duration@millisecond"),
                                ),
                            ),
                        ),
                        alias="aggregate_value",
                    ),
                ),
                SelectedExpression(
                    "transaction", column("transaction", None, "transaction")
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
                in_cond(column("project_id"), f.tuple(literal(11))),
                in_cond(column("org_id"), f.tuple(literal(1))),
                f.equals(column("use_case_id"), literal("transactions")),
                f.greaterOrEquals(
                    column("timestamp"), literal(datetime(2023, 11, 23, 18, 30))
                ),
                f.less(column("timestamp"), literal(datetime(2023, 11, 23, 22, 30))),
            ),
            groupby=[
                column("transaction", None, "transaction"),
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
                                column("value"),
                                f.equals(
                                    column("metric_id"),
                                    literal("d:transactions/duration@millisecond"),
                                ),
                            ),
                            f.sumIf(
                                column("value"),
                                f.equals(
                                    column("metric_id"),
                                    literal("d:transactions/duration@millisecond"),
                                ),
                            ),
                        ),
                        literal(100.0),
                        alias="aggregate_value",
                    ),
                )
            ],
            array_join=None,
            condition=None,
            groupby=None,
            having=None,
            order_by=None,
            limitby=None,
            limit=None,
            offset=0,
            totals=False,
            granularity=None,
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
                                column("value"),
                                f.equals(
                                    column("metric_id"),
                                    literal("d:transactions/duration@millisecond"),
                                ),
                            ),
                            f.sumIf(
                                column("value"),
                                f.equals(
                                    column("metric_id"),
                                    literal("d:transactions/duration@millisecond"),
                                ),
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
                in_cond(column("project_id"), f.tuple(literal(11))),
                in_cond(column("org_id"), f.tuple(literal(1))),
                f.equals(column("use_case_id"), literal("transactions")),
                f.greaterOrEquals(
                    column("timestamp"), literal(datetime(2023, 11, 23, 18, 30))
                ),
                f.less(column("timestamp"), literal(datetime(2023, 11, 23, 22, 30))),
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
    ),
    pytest.param(
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
                            f.sum(column("value")),
                            f.equals(
                                column("metric_id"),
                                literal("d:transactions/duration@millisecond"),
                            ),
                        ),
                        f.maxIf(
                            column("value"),
                            f.equals(
                                column("metric_id"),
                                literal("d:transactions/duration@millisecond"),
                            ),
                        ),
                        alias="aggregate_value",
                    ),
                )
            ],
            array_join=None,
            condition=None,
            groupby=None,
            having=None,
            order_by=None,
            limitby=None,
            limit=None,
            offset=0,
            totals=False,
            granularity=None,
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
                            f.sum(column("value")),
                            f.equals(
                                column("metric_id"),
                                literal("d:transactions/duration@millisecond"),
                            ),
                        ),
                        f.maxIf(
                            column("value"),
                            f.equals(
                                column("metric_id"),
                                literal("d:transactions/duration@millisecond"),
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
                in_cond(column("project_id"), f.tuple(literal(11))),
                in_cond(column("org_id"), f.tuple(literal(1))),
                f.equals(column("use_case_id"), literal("transactions")),
                f.greaterOrEquals(
                    column("timestamp"), literal(datetime(2023, 11, 23, 18, 30))
                ),
                f.less(column("timestamp"), literal(datetime(2023, 11, 23, 22, 30))),
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
    ),
    pytest.param(
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
                    "transaction", column("transaction", None, "transaction")
                ),
                SelectedExpression(
                    "status_code", column("status_code", None, "status_code")
                ),
            ],
            array_join=None,
            condition=and_cond(
                f.equals(
                    column("metric_id"), literal("d:transactions/duration@millisecond")
                ),
                in_cond(column("dist"), f.tuple(literal("dist1"), literal("dist2"))),
            ),
            groupby=[
                column("transaction", None, "transaction"),
                column("status_code", None, "status_code"),
            ],
            having=None,
            order_by=None,
            limitby=None,
            limit=None,
            offset=0,
            totals=False,
            granularity=None,
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
                    "transaction", column("transaction", None, "transaction")
                ),
                SelectedExpression(
                    "status_code", column("status_code", None, "status_code")
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
                in_cond(column("project_id"), f.tuple(literal(11))),
                in_cond(column("org_id"), f.tuple(literal(1))),
                f.equals(column("use_case_id"), literal("transactions")),
                f.greaterOrEquals(
                    column("timestamp"), literal(datetime(2023, 11, 23, 18, 30))
                ),
                f.less(column("timestamp"), literal(datetime(2023, 11, 23, 22, 30))),
                f.equals(
                    column("metric_id"), literal("d:transactions/duration@millisecond")
                ),
                in_cond(column("dist"), f.tuple(literal("dist1"), literal("dist2"))),
            ),
            groupby=[
                column("transaction", None, "transaction"),
                column("status_code", None, "status_code"),
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
        Query(
            from_clause=Entity(
                EntityKey.GENERIC_METRICS_DISTRIBUTIONS,
                get_entity(EntityKey.GENERIC_METRICS_DISTRIBUTIONS).get_data_model(),
            ),
            selected_columns=[
                SelectedExpression(
                    "aggregate_value", f.sum(column("value"), alias="aggregate_value")
                )
            ],
            array_join=None,
            condition=and_cond(
                f.equals(
                    column("metric_id"), literal("d:transactions/duration@millisecond")
                ),
                in_cond(column("dist"), f.tuple(literal("dist1"), literal("dist2"))),
            ),
            groupby=None,
            having=None,
            order_by=None,
            limitby=None,
            limit=None,
            offset=0,
            totals=False,
            granularity=None,
        ),
        Query(
            from_clause=Entity(
                EntityKey.GENERIC_METRICS_DISTRIBUTIONS,
                get_entity(EntityKey.GENERIC_METRICS_DISTRIBUTIONS).get_data_model(),
            ),
            selected_columns=[
                SelectedExpression(
                    "aggregate_value", f.sum(column("value"), alias="aggregate_value")
                )
            ],
            array_join=None,
            condition=and_cond(
                f.equals(column("granularity"), literal(60)),
                in_cond(column("project_id"), f.tuple(literal(1))),
                in_cond(column("org_id"), f.tuple(literal(1))),
                f.equals(column("use_case_id"), literal("transactions")),
                f.greaterOrEquals(
                    column("timestamp"), literal(datetime(2021, 1, 1, 0, 0))
                ),
                f.less(column("timestamp"), literal(datetime(2021, 1, 2, 0, 0))),
                f.equals(
                    column("metric_id"), literal("d:transactions/duration@millisecond")
                ),
                in_cond(column("dist"), f.tuple(literal("dist1"), literal("dist2"))),
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
    ),
    pytest.param(
        Query(
            from_clause=Entity(
                EntityKey.GENERIC_METRICS_DISTRIBUTIONS,
                get_entity(EntityKey.GENERIC_METRICS_DISTRIBUTIONS).get_data_model(),
            ),
            selected_columns=[
                SelectedExpression(
                    "aggregate_value", f.sum(column("value"), alias="aggregate_value")
                )
            ],
            array_join=None,
            condition=f.equals(
                column("metric_id"), literal("d:transactions/duration@millisecond")
            ),
            groupby=None,
            having=None,
            order_by=None,
            limitby=None,
            limit=None,
            offset=0,
            totals=False,
            granularity=None,
        ),
        Query(
            from_clause=Entity(
                EntityKey.GENERIC_METRICS_DISTRIBUTIONS,
                get_entity(EntityKey.GENERIC_METRICS_DISTRIBUTIONS).get_data_model(),
            ),
            selected_columns=[
                SelectedExpression(
                    "aggregate_value", f.sum(column("value"), alias="aggregate_value")
                )
            ],
            array_join=None,
            condition=and_cond(
                f.equals(column("granularity"), literal(60)),
                in_cond(column("project_id"), f.tuple(literal(1))),
                in_cond(column("org_id"), f.tuple(literal(1))),
                f.equals(column("use_case_id"), literal("transactions")),
                f.greaterOrEquals(
                    column("timestamp"), literal(datetime(2021, 1, 1, 0, 0))
                ),
                f.less(column("timestamp"), literal(datetime(2021, 1, 2, 0, 0))),
                f.equals(
                    column("metric_id"), literal("d:transactions/duration@millisecond")
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
    ),
    pytest.param(
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
                    "transaction", column("transaction", None, "transaction")
                ),
            ],
            array_join=None,
            condition=and_cond(
                f.equals(column("metric_id"), literal("s:transactions/user@none")),
                f.notIn(column("dist"), f.tuple(literal("dist1"), literal("dist2"))),
                f.equals(column("foo"), literal("bar")),
            ),
            groupby=[column("transaction", None, "transaction")],
            having=None,
            order_by=None,
            limitby=None,
            limit=None,
            offset=0,
            totals=False,
            granularity=None,
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
                    "transaction", column("transaction", None, "transaction")
                ),
            ],
            array_join=None,
            condition=and_cond(
                f.equals(column("granularity"), literal(3600)),
                in_cond(column("project_id"), f.tuple(literal(1))),
                in_cond(column("org_id"), f.tuple(literal(1))),
                f.equals(column("use_case_id"), literal("transactions")),
                f.greaterOrEquals(
                    column("timestamp"), literal(datetime(2021, 1, 1, 1, 36))
                ),
                f.less(column("timestamp"), literal(datetime(2021, 1, 5, 4, 15))),
                f.equals(column("metric_id"), literal("s:transactions/user@none")),
                f.notIn(column("dist"), f.tuple(literal("dist1"), literal("dist2"))),
                f.equals(column("foo"), literal("bar")),
            ),
            groupby=[column("transaction", None, "transaction")],
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
                    "transaction", column("transaction", None, "transaction")
                ),
                SelectedExpression(
                    "status_code", column("status_code", None, "status_code")
                ),
            ],
            array_join=None,
            condition=and_cond(
                f.equals(
                    column("metric_id"), literal("d:transactions/duration@millisecond")
                ),
                in_cond(column("dist"), f.tuple(literal("dist1"), literal("dist2"))),
            ),
            groupby=[
                column("transaction", None, "transaction"),
                column("status_code", None, "status_code"),
            ],
            having=None,
            order_by=None,
            limitby=None,
            limit=None,
            offset=0,
            totals=False,
            granularity=None,
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
                    "transaction", column("transaction", None, "transaction")
                ),
                SelectedExpression(
                    "status_code", column("status_code", None, "status_code")
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
                in_cond(column("project_id"), f.tuple(literal(11))),
                in_cond(column("org_id"), f.tuple(literal(1))),
                f.equals(column("use_case_id"), literal("transactions")),
                f.greaterOrEquals(
                    column("timestamp"), literal(datetime(2023, 11, 23, 18, 30))
                ),
                f.less(column("timestamp"), literal(datetime(2023, 11, 23, 22, 30))),
                f.equals(
                    column("metric_id"), literal("d:transactions/duration@millisecond")
                ),
                in_cond(column("dist"), f.tuple(literal("dist1"), literal("dist2"))),
            ),
            groupby=[
                column("transaction", None, "transaction"),
                column("status_code", None, "status_code"),
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
        Query(
            from_clause=Entity(
                EntityKey.METRICS_DISTRIBUTIONS,
                get_entity(EntityKey.METRICS_DISTRIBUTIONS).get_data_model(),
            ),
            selected_columns=[
                SelectedExpression(
                    "aggregate_value", f.sum(column("value"), alias="aggregate_value")
                ),
                SelectedExpression("release", column("release", None, "release")),
            ],
            array_join=None,
            condition=and_cond(
                f.equals(column("metric_id"), literal("d:sessions/duration@second")),
                in_cond(column("release"), f.tuple(literal("foo"), literal("bar"))),
            ),
            groupby=[column("release", None, "release")],
            having=None,
            order_by=None,
            limitby=None,
            limit=None,
            offset=0,
            totals=False,
            granularity=None,
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
                SelectedExpression("release", column("release", None, "release")),
            ],
            array_join=None,
            condition=and_cond(
                f.equals(column("granularity"), literal(60)),
                in_cond(column("project_id"), f.tuple(literal(1))),
                in_cond(column("org_id"), f.tuple(literal(1))),
                f.equals(column("use_case_id"), literal("sessions")),
                f.greaterOrEquals(
                    column("timestamp"), literal(datetime(2021, 1, 1, 0, 0))
                ),
                f.less(column("timestamp"), literal(datetime(2021, 1, 2, 0, 0))),
                f.equals(column("metric_id"), literal("d:sessions/duration@second")),
                in_cond(column("release"), f.tuple(literal("foo"), literal("bar"))),
            ),
            groupby=[column("release", None, "release")],
            having=None,
            order_by=[OrderBy(OrderByDirection.ASC, column("aggregate_value"))],
            limitby=None,
            limit=1000,
            offset=0,
            totals=False,
            granularity=None,
        ),
    ),
    pytest.param(
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
                    "transaction", column("transaction", None, "transaction")
                ),
            ],
            array_join=None,
            condition=and_cond(
                f.equals(
                    column("metric_id"), literal("d:transactions/duration@millisecond")
                ),
                f.equals(
                    column("bar"),
                    literal(
                        " !\"#$%&'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\\\]^_`abcdefghijklmnopqrstuvwxyz{|}~"
                    ),
                ),
            ),
            groupby=[column("transaction", None, "transaction")],
            having=None,
            order_by=None,
            limitby=None,
            limit=None,
            offset=0,
            totals=False,
            granularity=None,
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
                    "transaction", column("transaction", None, "transaction")
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
                in_cond(column("project_id"), f.tuple(literal(1))),
                in_cond(column("org_id"), f.tuple(literal(1))),
                f.equals(column("use_case_id"), literal("transactions")),
                f.greaterOrEquals(
                    column("timestamp"), literal(datetime(2024, 1, 7, 13, 35))
                ),
                f.less(column("timestamp"), literal(datetime(2024, 1, 8, 13, 40))),
                f.equals(
                    column("metric_id"), literal("d:transactions/duration@millisecond")
                ),
                f.equals(
                    column("bar"),
                    literal(
                        " !\"#$%&'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\\\]^_`abcdefghijklmnopqrstuvwxyz{|}~"
                    ),
                ),
            ),
            groupby=[
                column("transaction", None, "transaction"),
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
    ),
    pytest.param(
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
                )
            ],
            array_join=None,
            condition=and_cond(
                f.equals(
                    column("metric_id"), literal("d:transactions/duration@millisecond")
                ),
                in_cond(column("dist"), f.tuple(literal("dist1"), literal("dist2"))),
            ),
            groupby=None,
            having=None,
            order_by=None,
            limitby=None,
            limit=None,
            offset=0,
            totals=False,
            granularity=None,
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
                )
            ],
            array_join=None,
            condition=and_cond(
                f.equals(column("granularity"), literal(60)),
                in_cond(column("project_id"), f.tuple(literal(1))),
                in_cond(column("org_id"), f.tuple(literal(1))),
                f.equals(column("use_case_id"), literal("transactions")),
                f.greaterOrEquals(
                    column("timestamp"), literal(datetime(2021, 1, 1, 0, 0))
                ),
                f.less(column("timestamp"), literal(datetime(2021, 1, 2, 0, 0))),
                f.equals(
                    column("metric_id"), literal("d:transactions/duration@millisecond")
                ),
                in_cond(column("dist"), f.tuple(literal("dist1"), literal("dist2"))),
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
    ),
    pytest.param(
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
            condition=f.equals(
                column("metric_id"), literal("s:transactions/user@none")
            ),
            groupby=None,
            having=None,
            order_by=None,
            limitby=None,
            limit=None,
            offset=0,
            totals=False,
            granularity=None,
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
                )
            ],
            array_join=None,
            condition=and_cond(
                f.equals(column("granularity"), literal(3600)),
                in_cond(column("project_id"), f.tuple(literal(1))),
                in_cond(column("org_id"), f.tuple(literal(1))),
                f.equals(column("use_case_id"), literal("transactions")),
                f.greaterOrEquals(
                    column("timestamp"), literal(datetime(2021, 1, 1, 1, 36))
                ),
                f.less(column("timestamp"), literal(datetime(2021, 1, 5, 4, 15))),
                f.equals(column("metric_id"), literal("s:transactions/user@none")),
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
        Query(
            from_clause=Entity(
                EntityKey.GENERIC_METRICS_DISTRIBUTIONS,
                get_entity(EntityKey.GENERIC_METRICS_DISTRIBUTIONS).get_data_model(),
            ),
            selected_columns=[
                SelectedExpression(
                    "aggregate_value", f.avg(column("value"), alias="aggregate_value")
                )
            ],
            array_join=None,
            condition=and_cond(
                f.equals(
                    column("metric_id"),
                    literal(
                        "d:custom/sentry.event_manager.save_transactions.fetch_organizations@second"
                    ),
                ),
                f.equals(column("event_type"), literal("transaction")),
                f.equals(
                    column("transaction"),
                    literal("sentry.tasks.store.save_event_transaction"),
                ),
            ),
            groupby=None,
            having=None,
            order_by=None,
            limitby=None,
            limit=None,
            offset=0,
            totals=False,
            granularity=None,
        ),
        Query(
            from_clause=Entity(
                EntityKey.GENERIC_METRICS_DISTRIBUTIONS,
                get_entity(EntityKey.GENERIC_METRICS_DISTRIBUTIONS).get_data_model(),
            ),
            selected_columns=[
                SelectedExpression(
                    "aggregate_value", f.avg(column("value"), alias="aggregate_value")
                )
            ],
            array_join=None,
            condition=and_cond(
                f.equals(column("granularity"), literal(60)),
                in_cond(column("project_id"), f.tuple(literal(1))),
                in_cond(column("org_id"), f.tuple(literal(1))),
                f.equals(column("use_case_id"), literal("custom")),
                f.greaterOrEquals(
                    column("timestamp"), literal(datetime(2021, 1, 1, 0, 0))
                ),
                f.less(column("timestamp"), literal(datetime(2021, 1, 2, 0, 0))),
                f.equals(
                    column("metric_id"),
                    literal(
                        "d:custom/sentry.event_manager.save_transactions.fetch_organizations@second"
                    ),
                ),
                f.equals(column("event_type"), literal("transaction")),
                f.equals(
                    column("transaction"),
                    literal("sentry.tasks.store.save_event_transaction"),
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
        Query(
            from_clause=Entity(
                EntityKey.GENERIC_METRICS_DISTRIBUTIONS,
                get_entity(EntityKey.GENERIC_METRICS_DISTRIBUTIONS).get_data_model(),
            ),
            selected_columns=[
                SelectedExpression(
                    "aggregate_value", f.sum(column("value"), alias="aggregate_value")
                )
            ],
            array_join=None,
            condition=and_cond(
                f.equals(
                    column("metric_id"), literal("d:transactions/duration@millisecond")
                ),
                in_cond(column("dist"), f.tuple(literal("dist1"), literal("dist2"))),
            ),
            groupby=None,
            having=None,
            order_by=None,
            limitby=None,
            limit=None,
            offset=0,
            totals=False,
            granularity=None,
        ),
        None,
    ),
    pytest.param(
        Query(
            from_clause=Entity(
                EntityKey.GENERIC_METRICS_DISTRIBUTIONS,
                get_entity(EntityKey.GENERIC_METRICS_DISTRIBUTIONS).get_data_model(),
            ),
            selected_columns=[
                SelectedExpression(
                    "aggregate_value", f.sum(column("value"), alias="aggregate_value")
                )
            ],
            array_join=None,
            condition=and_cond(
                f.equals(
                    column("metric_id"), literal("d:transactions/duration@millisecond")
                ),
                in_cond(column("dist"), f.tuple(literal("dist1"), literal("dist2"))),
            ),
            groupby=None,
            having=None,
            order_by=None,
            limitby=None,
            limit=None,
            offset=0,
            totals=False,
            granularity=None,
        ),
        None,
    ),
    pytest.param(
        Query(
            from_clause=Entity(
                EntityKey.GENERIC_METRICS_DISTRIBUTIONS,
                get_entity(EntityKey.GENERIC_METRICS_DISTRIBUTIONS).get_data_model(),
            ),
            selected_columns=[
                SelectedExpression(
                    "aggregate_value", f.sum(column("value"), alias="aggregate_value")
                )
            ],
            array_join=None,
            condition=and_cond(
                f.equals(
                    column("metric_id"), literal("d:transactions/duration@millisecond")
                ),
                in_cond(column("dist"), f.tuple(literal("dist1"), literal("dist2"))),
            ),
            groupby=None,
            having=None,
            order_by=None,
            limitby=None,
            limit=None,
            offset=0,
            totals=False,
            granularity=None,
        ),
        None,
    ),
    pytest.param(
        Query(
            from_clause=Entity(
                EntityKey.GENERIC_METRICS_DISTRIBUTIONS,
                get_entity(EntityKey.GENERIC_METRICS_DISTRIBUTIONS).get_data_model(),
            ),
            selected_columns=[
                SelectedExpression(
                    "aggregate_value", f.sum(column("value"), alias="aggregate_value")
                )
            ],
            array_join=None,
            condition=and_cond(
                f.equals(
                    column("metric_id"), literal("d:transactions/duration@millisecond")
                ),
                in_cond(column("dist"), f.tuple(literal("dist1"), literal("dist2"))),
            ),
            groupby=None,
            having=None,
            order_by=None,
            limitby=None,
            limit=None,
            offset=0,
            totals=False,
            granularity=None,
        ),
        None,
    ),
    pytest.param(
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
                                column("value"),
                                and_cond(
                                    f.equals(
                                        column("transaction"),
                                        literal(
                                            "getsentry.tasks.calculate_spike_projections"
                                        ),
                                    ),
                                    f.equals(
                                        column("metric_id"),
                                        literal("d:transactions/duration@millisecond"),
                                    ),
                                ),
                            ),
                            literal(100.0),
                        ),
                        literal(100.0),
                        alias="aggregate_value",
                    ),
                )
            ],
            array_join=None,
            condition=None,
            groupby=None,
            having=None,
            order_by=None,
            limitby=None,
            limit=None,
            offset=0,
            totals=False,
            granularity=None,
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
                                column("value"),
                                and_cond(
                                    f.equals(
                                        column("transaction"),
                                        literal(
                                            "getsentry.tasks.calculate_spike_projections"
                                        ),
                                    ),
                                    f.equals(
                                        column("metric_id"),
                                        literal("d:transactions/duration@millisecond"),
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
                in_cond(column("project_id"), f.tuple(literal(1))),
                in_cond(column("org_id"), f.tuple(literal(1))),
                f.equals(column("use_case_id"), literal("'transactions'")),
                f.greaterOrEquals(
                    column("timestamp"), literal(datetime(2024, 4, 8, 5, 48))
                ),
                f.less(column("timestamp"), literal(datetime(2024, 4, 8, 6, 49))),
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
    ),
]


@pytest.mark.parametrize("qin, expected", test_cases)
def test_autogenerated(mql: str, qin, expected: Query) -> None:
    actual = populate_query_from_mql_context(
        *qin,
    )
    assert actual == expected
