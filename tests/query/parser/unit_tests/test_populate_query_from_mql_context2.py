import re
from datetime import datetime
from typing import Any

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
from snuba.query.mql.mql_context import MetricsScope, MQLContext, Rollup
from snuba.query.mql.parser import parse_mql_query_body, populate_query_from_mql_context
from snuba.query.parser.exceptions import ParsingException

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
        ),
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
            MQLContext(
                start="2023-11-23T18:30:00",
                end="2023-11-23T22:30:00",
                rollup=Rollup(
                    granularity=60, interval=60, with_totals="False", orderby=None
                ),
                scope=MetricsScope(
                    org_ids=[1], project_ids=[11], use_case_id="transactions"
                ),
                indexer_mappings={
                    "d:transactions/duration@millisecond": 123456,
                    "status_code": 222222,
                    "transaction": 333333,
                },
                limit=None,
                offset=None,
            ),
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
                                        f.equals(column("status_code"), literal("200")),
                                        f.equals(
                                            column("metric_id"),
                                            literal(
                                                "d:transactions/duration@millisecond"
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
        ),
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
                                        f.equals(column("status_code"), literal("200")),
                                        f.equals(
                                            column("metric_id"),
                                            literal(
                                                "d:transactions/duration@millisecond"
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
            MQLContext(
                start="2023-11-23T18:30:00",
                end="2023-11-23T22:30:00",
                rollup=Rollup(
                    granularity=60, interval=60, with_totals="False", orderby=None
                ),
                scope=MetricsScope(
                    org_ids=[1], project_ids=[11], use_case_id="transactions"
                ),
                indexer_mappings={
                    "d:transactions/duration@millisecond": 123456,
                    "status_code": 222222,
                    "transaction": 333333,
                },
                limit=None,
                offset=None,
            ),
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
        ),
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
            MQLContext(
                start="2023-11-23T18:30:00",
                end="2023-11-23T22:30:00",
                rollup=Rollup(
                    granularity=60, interval=60, with_totals="False", orderby=None
                ),
                scope=MetricsScope(
                    org_ids=[1], project_ids=[11], use_case_id="transactions"
                ),
                indexer_mappings={
                    "d:transactions/duration@millisecond": 123456,
                    "status_code": 222222,
                    "transaction": 333333,
                },
                limit=None,
                offset=None,
            ),
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
                                        f.equals(column("status_code"), literal("200")),
                                        f.equals(
                                            column("metric_id"),
                                            literal(
                                                "d:transactions/duration@millisecond"
                                            ),
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
        ),
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
                                        f.equals(column("status_code"), literal("200")),
                                        f.equals(
                                            column("metric_id"),
                                            literal(
                                                "d:transactions/duration@millisecond"
                                            ),
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
            MQLContext(
                start="2023-11-23T18:30:00",
                end="2023-11-23T22:30:00",
                rollup=Rollup(
                    granularity=60, interval=60, with_totals="False", orderby=None
                ),
                scope=MetricsScope(
                    org_ids=[1], project_ids=[11], use_case_id="transactions"
                ),
                indexer_mappings={
                    "d:transactions/duration@millisecond": 123456,
                    "status_code": 222222,
                    "transaction": 333333,
                },
                limit=None,
                offset=None,
            ),
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
                                            literal(
                                                "d:transactions/duration@millisecond"
                                            ),
                                        ),
                                    ),
                                    f.avgIf(
                                        column("value"),
                                        f.equals(
                                            column("metric_id"),
                                            literal(
                                                "d:transactions/duration@millisecond"
                                            ),
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
        ),
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
                                            literal(
                                                "d:transactions/duration@millisecond"
                                            ),
                                        ),
                                    ),
                                    f.avgIf(
                                        column("value"),
                                        f.equals(
                                            column("metric_id"),
                                            literal(
                                                "d:transactions/duration@millisecond"
                                            ),
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
            MQLContext(
                start="2023-11-23T18:30:00",
                end="2023-11-23T22:30:00",
                rollup=Rollup(
                    granularity=60, interval=60, with_totals="False", orderby=None
                ),
                scope=MetricsScope(
                    org_ids=[1], project_ids=[11], use_case_id="transactions"
                ),
                indexer_mappings={
                    "d:transactions/duration@millisecond": 123456,
                    "status_code": 222222,
                    "transaction": 333333,
                },
                limit=None,
                offset=None,
            ),
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
        ),
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
            MQLContext(
                start="2023-11-23T18:30:00",
                end="2023-11-23T22:30:00",
                rollup=Rollup(
                    granularity=60, interval=60, with_totals="False", orderby=None
                ),
                scope=MetricsScope(
                    org_ids=[1], project_ids=[11], use_case_id="transactions"
                ),
                indexer_mappings={
                    "d:transactions/duration@millisecond": 123456,
                    "status_code": 222222,
                    "transaction": 333333,
                },
                limit=None,
                offset=None,
            ),
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
        ),
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
            MQLContext(
                start="2023-11-23T18:30:00",
                end="2023-11-23T22:30:00",
                rollup=Rollup(
                    granularity=60, interval=60, with_totals="False", orderby=None
                ),
                scope=MetricsScope(
                    org_ids=[1], project_ids=[11], use_case_id="transactions"
                ),
                indexer_mappings={
                    "d:transactions/duration@millisecond": 123456,
                    "status_code": 222222,
                    "transaction": 333333,
                },
                limit=None,
                offset=None,
            ),
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
        ),
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
            MQLContext(
                start="2023-11-23T18:30:00",
                end="2023-11-23T22:30:00",
                rollup=Rollup(
                    granularity=60, interval=60, with_totals="False", orderby=None
                ),
                scope=MetricsScope(
                    org_ids=[1], project_ids=[11], use_case_id="transactions"
                ),
                indexer_mappings={
                    "d:transactions/duration@millisecond": 123456,
                    "status_code": 222222,
                    "transaction": 333333,
                },
                limit=None,
                offset=None,
            ),
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
        ),
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
            MQLContext(
                start="2023-11-23T18:30:00",
                end="2023-11-23T22:30:00",
                rollup=Rollup(
                    granularity=60, interval=60, with_totals="False", orderby=None
                ),
                scope=MetricsScope(
                    org_ids=[1], project_ids=[11], use_case_id="transactions"
                ),
                indexer_mappings={
                    "d:transactions/duration@millisecond": 123456,
                    "status_code": 222222,
                    "transaction": 333333,
                },
                limit=None,
                offset=None,
            ),
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
                        "transaction", column("transaction", None, "transaction")
                    ),
                    SelectedExpression(
                        "status_code", column("status_code", None, "status_code")
                    ),
                ],
                array_join=None,
                condition=and_cond(
                    f.equals(
                        column("metric_id"),
                        literal("d:transactions/duration@millisecond"),
                    ),
                    in_cond(
                        column("dist"), f.tuple(literal("dist1"), literal("dist2"))
                    ),
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
        ),
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
                                f.equals(
                                    column("metric_id"),
                                    literal("d:transactions/duration@millisecond"),
                                ),
                                in_cond(
                                    column("dist"),
                                    f.tuple(literal("dist1"), literal("dist2")),
                                ),
                            ),
                        ),
                    ),
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
            MQLContext(
                start="2023-11-23T18:30:00",
                end="2023-11-23T22:30:00",
                rollup=Rollup(
                    granularity=60, interval=60, with_totals="False", orderby=None
                ),
                scope=MetricsScope(
                    org_ids=[1], project_ids=[11], use_case_id="transactions"
                ),
                indexer_mappings={
                    "d:transactions/duration@millisecond": 123456,
                    "dist": 888,
                    "transaction": 111111,
                    "status_code": 222222,
                },
                limit=None,
                offset=None,
            ),
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
                    f.equals(
                        column("metric_id"),
                        literal("d:transactions/duration@millisecond"),
                    ),
                    in_cond(
                        column("dist"), f.tuple(literal("dist1"), literal("dist2"))
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
            {
                "start": "2021-01-01T00:00:00",
                "end": "2021-01-02T00:00:00",
                "rollup": {
                    "orderby": "ASC",
                    "granularity": 60,
                    "interval": None,
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
        ),
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
                                f.equals(
                                    column("metric_id"),
                                    literal("d:transactions/duration@millisecond"),
                                ),
                                in_cond(
                                    column("dist"),
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
            MQLContext(
                start="2021-01-01T00:00:00",
                end="2021-01-02T00:00:00",
                rollup=Rollup(
                    granularity=60, interval=None, with_totals=None, orderby="ASC"
                ),
                scope=MetricsScope(
                    org_ids=[1], project_ids=[1], use_case_id="transactions"
                ),
                indexer_mappings={
                    "d:transactions/duration@millisecond": 123456,
                    "dist": 888,
                },
                limit=None,
                offset=None,
            ),
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
            {
                "start": "2021-01-01T00:00:00",
                "end": "2021-01-02T00:00:00",
                "rollup": {
                    "orderby": "ASC",
                    "granularity": 60,
                    "interval": None,
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
        ),
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
                            f.equals(
                                column("metric_id"),
                                literal("d:transactions/duration@millisecond"),
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
            MQLContext(
                start="2021-01-01T00:00:00",
                end="2021-01-02T00:00:00",
                rollup=Rollup(
                    granularity=60, interval=None, with_totals=None, orderby="ASC"
                ),
                scope=MetricsScope(
                    org_ids=[1], project_ids=[1], use_case_id="transactions"
                ),
                indexer_mappings={"d:transactions/duration@millisecond": 123456},
                limit=None,
                offset=None,
            ),
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
                        "transaction", column("transaction", None, "transaction")
                    ),
                ],
                array_join=None,
                condition=and_cond(
                    f.equals(column("metric_id"), literal("s:transactions/user@none")),
                    and_cond(
                        f.notIn(
                            column("dist"), f.tuple(literal("dist1"), literal("dist2"))
                        ),
                        f.equals(column("foo"), literal("bar")),
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
            {
                "start": "2021-01-01T01:36:00",
                "end": "2021-01-05T04:15:00",
                "rollup": {
                    "orderby": None,
                    "granularity": 3600,
                    "interval": None,
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
        ),
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
                        "transaction", column("transaction", None, "transaction")
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
                                f.equals(
                                    column("metric_id"),
                                    literal("s:transactions/user@none"),
                                ),
                                and_cond(
                                    f.notIn(
                                        column("dist"),
                                        f.tuple(literal("dist1"), literal("dist2")),
                                    ),
                                    f.equals(column("foo"), literal("bar")),
                                ),
                            ),
                        ),
                    ),
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
            MQLContext(
                start="2021-01-01T01:36:00",
                end="2021-01-05T04:15:00",
                rollup=Rollup(
                    granularity=3600, interval=None, with_totals=None, orderby=None
                ),
                scope=MetricsScope(
                    org_ids=[1], project_ids=[1], use_case_id="transactions"
                ),
                indexer_mappings={
                    "transaction.user": "s:transactions/user@none",
                    "s:transactions/user@none": 567890,
                    "dist": 888888,
                    "foo": 777777,
                    "transaction": 111111,
                },
                limit=100,
                offset=3,
            ),
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
                        "transaction", column("transaction", None, "transaction")
                    ),
                    SelectedExpression(
                        "status_code", column("status_code", None, "status_code")
                    ),
                ],
                array_join=None,
                condition=and_cond(
                    f.equals(
                        column("metric_id"),
                        literal("d:transactions/duration@millisecond"),
                    ),
                    in_cond(
                        column("dist"), f.tuple(literal("dist1"), literal("dist2"))
                    ),
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
        ),
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
                                f.equals(
                                    column("metric_id"),
                                    literal("d:transactions/duration@millisecond"),
                                ),
                                in_cond(
                                    column("dist"),
                                    f.tuple(literal("dist1"), literal("dist2")),
                                ),
                            ),
                        ),
                    ),
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
            MQLContext(
                start="2023-11-23T18:30:00",
                end="2023-11-23T22:30:00",
                rollup=Rollup(
                    granularity=60, interval=60, with_totals="False", orderby=None
                ),
                scope=MetricsScope(
                    org_ids=[1], project_ids=[11], use_case_id="transactions"
                ),
                indexer_mappings={
                    "d:transactions/duration@millisecond": 123456,
                    "dist": 888,
                    "transaction": 111111,
                    "status_code": 222222,
                },
                limit=None,
                offset=None,
            ),
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
                    SelectedExpression("release", column("release", None, "release")),
                ],
                array_join=None,
                condition=and_cond(
                    f.equals(
                        column("metric_id"), literal("d:sessions/duration@second")
                    ),
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
            {
                "start": "2021-01-01T00:00:00",
                "end": "2021-01-02T00:00:00",
                "rollup": {
                    "orderby": "ASC",
                    "granularity": 60,
                    "interval": None,
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
        ),
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
                    SelectedExpression("release", column("release", None, "release")),
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
                                f.equals(
                                    column("metric_id"),
                                    literal("d:sessions/duration@second"),
                                ),
                                in_cond(
                                    column("release"),
                                    f.tuple(literal("foo"), literal("bar")),
                                ),
                            ),
                        ),
                    ),
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
            MQLContext(
                start="2021-01-01T00:00:00",
                end="2021-01-02T00:00:00",
                rollup=Rollup(
                    granularity=60, interval=None, with_totals=None, orderby="ASC"
                ),
                scope=MetricsScope(
                    org_ids=[1], project_ids=[1], use_case_id="sessions"
                ),
                indexer_mappings={
                    "d:sessions/duration@second": 123456,
                    "release": 111,
                    "foo": 222,
                    "bar": 333,
                },
                limit=None,
                offset=None,
            ),
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
                        "transaction", column("transaction", None, "transaction")
                    ),
                ],
                array_join=None,
                condition=and_cond(
                    f.equals(
                        column("metric_id"),
                        literal("d:transactions/duration@millisecond"),
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
        ),
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
                                f.equals(
                                    column("metric_id"),
                                    literal("d:transactions/duration@millisecond"),
                                ),
                                f.equals(
                                    column("bar"),
                                    literal(
                                        " !\"#$%&'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\\\]^_`abcdefghijklmnopqrstuvwxyz{|}~"
                                    ),
                                ),
                            ),
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
            MQLContext(
                start="2024-01-07T13:35:00+00:00",
                end="2024-01-08T13:40:00+00:00",
                rollup=Rollup(
                    granularity=60, interval=300, with_totals=None, orderby=None
                ),
                scope=MetricsScope(
                    org_ids=[1], project_ids=[1], use_case_id="transactions"
                ),
                indexer_mappings={
                    "d:transactions/duration@millisecond": 123456,
                    " !\"#$%&'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz{|}~": 78910,
                    "bar": 111213,
                    "transaction": 141516,
                },
                limit=10000,
                offset=None,
            ),
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
                    f.equals(
                        column("metric_id"),
                        literal("d:transactions/duration@millisecond"),
                    ),
                    in_cond(
                        column("dist"), f.tuple(literal("dist1"), literal("dist2"))
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
            {
                "start": "2021-01-01T00:00:00",
                "end": "2021-01-02T00:00:00",
                "rollup": {
                    "orderby": "ASC",
                    "granularity": 60,
                    "interval": None,
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
        ),
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
                                f.equals(
                                    column("metric_id"),
                                    literal("d:transactions/duration@millisecond"),
                                ),
                                in_cond(
                                    column("dist"),
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
            MQLContext(
                start="2021-01-01T00:00:00",
                end="2021-01-02T00:00:00",
                rollup=Rollup(
                    granularity=60, interval=None, with_totals=None, orderby="ASC"
                ),
                scope=MetricsScope(
                    org_ids=[1], project_ids=[1], use_case_id="transactions"
                ),
                indexer_mappings={
                    "d:transactions/duration@millisecond": 123456,
                    "dist": 888,
                },
                limit=None,
                offset=None,
            ),
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
            {
                "start": "2021-01-01T01:36:00",
                "end": "2021-01-05T04:15:00",
                "rollup": {
                    "orderby": None,
                    "granularity": 3600,
                    "interval": None,
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
        ),
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
                            f.equals(
                                column("metric_id"), literal("s:transactions/user@none")
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
            MQLContext(
                start="2021-01-01T01:36:00",
                end="2021-01-05T04:15:00",
                rollup=Rollup(
                    granularity=3600, interval=None, with_totals=None, orderby=None
                ),
                scope=MetricsScope(
                    org_ids=[1], project_ids=[1], use_case_id="transactions"
                ),
                indexer_mappings={
                    "transaction.user": "s:transactions/user@none",
                    "s:transactions/user@none": 567890,
                    "dist": 888888,
                    "foo": 777777,
                    "transaction": 111111,
                },
                limit=100,
                offset=3,
            ),
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
                    f.equals(
                        column("metric_id"),
                        literal(
                            "d:custom/sentry.event_manager.save_transactions.fetch_organizations@second"
                        ),
                    ),
                    and_cond(
                        f.equals(column("event_type"), literal("transaction")),
                        f.equals(
                            column("transaction"),
                            literal("sentry.tasks.store.save_event_transaction"),
                        ),
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
            {
                "start": "2021-01-01T00:00:00",
                "end": "2021-01-02T00:00:00",
                "rollup": {
                    "orderby": None,
                    "granularity": 60,
                    "interval": None,
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
        ),
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
                                f.equals(
                                    column("metric_id"),
                                    literal(
                                        "d:custom/sentry.event_manager.save_transactions.fetch_organizations@second"
                                    ),
                                ),
                                and_cond(
                                    f.equals(
                                        column("event_type"), literal("transaction")
                                    ),
                                    f.equals(
                                        column("transaction"),
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
            MQLContext(
                start="2021-01-01T00:00:00",
                end="2021-01-02T00:00:00",
                rollup=Rollup(
                    granularity=60, interval=None, with_totals=None, orderby=None
                ),
                scope=MetricsScope(org_ids=[1], project_ids=[1], use_case_id="custom"),
                indexer_mappings={
                    "d:custom/sentry.event_manager.save_transactions.fetch_organizations@second": 111111,
                    "event_type": 222222,
                    "transaction": 333333,
                },
                limit=None,
                offset=None,
            ),
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
                                            column("transaction"),
                                            literal(
                                                "getsentry.tasks.calculate_spike_projections"
                                            ),
                                        ),
                                        f.equals(
                                            column("metric_id"),
                                            literal(
                                                "d:transactions/duration@millisecond"
                                            ),
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
        ),
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
                                            column("transaction"),
                                            literal(
                                                "getsentry.tasks.calculate_spike_projections"
                                            ),
                                        ),
                                        f.equals(
                                            column("metric_id"),
                                            literal(
                                                "d:transactions/duration@millisecond"
                                            ),
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
            MQLContext(
                start="2024-04-08T05:48:00+00:00",
                end="2024-04-08T06:49:00+00:00",
                rollup=Rollup(
                    granularity=60, interval=60, with_totals=None, orderby=None
                ),
                scope=MetricsScope(
                    org_ids=[1], project_ids=[1], use_case_id="'transactions'"
                ),
                indexer_mappings={
                    "d:transactions/duration@millisecond": 9223372036854775909,
                    "transaction": 9223372036854776020,
                },
                limit=10000,
                offset=None,
            ),
        ),
    ),
]


@pytest.mark.parametrize("theinput, expected", test_cases)
def test_autogenerated(
    theinput: tuple[Query, dict[str, Any]], expected: tuple[Query, MQLContext]
) -> None:
    actual = populate_query_from_mql_context(*theinput)
    assert actual == expected


failure_cases = [
    (
        "('sum(`d:transactions/duration@millisecond`){dist:[\"dist1\", \"dist2\"]}', get_dataset('generic_metrics'))",
        "Query(\n        from_clause=Entity(EntityKey.GENERIC_METRICS_DISTRIBUTIONS,get_entity(EntityKey.GENERIC_METRICS_DISTRIBUTIONS).get_data_model()),\n        selected_columns=[SelectedExpression('aggregate_value', f.sum(column('value'), alias='aggregate_value'))],\n        array_join=None,\n        condition=and_cond(f.equals(column('metric_id'), literal('d:transactions/duration@millisecond')), in_cond(column('dist'), f.tuple(literal('dist1'), literal('dist2')))),\n        groupby=None,\n        having=None,\n        order_by=None,\n        limitby=None,\n        limit=None,\n        offset=0,\n        totals=False,\n        granularity=None,\n    )",
    ),
    (
        "('sum(`d:transactions/duration@millisecond`){dist:[\"dist1\", \"dist2\"]}', get_dataset('generic_metrics'))",
        "Query(\n        from_clause=Entity(EntityKey.GENERIC_METRICS_DISTRIBUTIONS,get_entity(EntityKey.GENERIC_METRICS_DISTRIBUTIONS).get_data_model()),\n        selected_columns=[SelectedExpression('aggregate_value', f.sum(column('value'), alias='aggregate_value'))],\n        array_join=None,\n        condition=and_cond(f.equals(column('metric_id'), literal('d:transactions/duration@millisecond')), in_cond(column('dist'), f.tuple(literal('dist1'), literal('dist2')))),\n        groupby=None,\n        having=None,\n        order_by=None,\n        limitby=None,\n        limit=None,\n        offset=0,\n        totals=False,\n        granularity=None,\n    )",
    ),
    (
        "('sum(`d:transactions/duration@millisecond`){dist:[\"dist1\", \"dist2\"]}', get_dataset('generic_metrics'))",
        "Query(\n        from_clause=Entity(EntityKey.GENERIC_METRICS_DISTRIBUTIONS,get_entity(EntityKey.GENERIC_METRICS_DISTRIBUTIONS).get_data_model()),\n        selected_columns=[SelectedExpression('aggregate_value', f.sum(column('value'), alias='aggregate_value'))],\n        array_join=None,\n        condition=and_cond(f.equals(column('metric_id'), literal('d:transactions/duration@millisecond')), in_cond(column('dist'), f.tuple(literal('dist1'), literal('dist2')))),\n        groupby=None,\n        having=None,\n        order_by=None,\n        limitby=None,\n        limit=None,\n        offset=0,\n        totals=False,\n        granularity=None,\n    )",
    ),
    (
        "('sum(`d:transactions/duration@millisecond`){dist:[\"dist1\", \"dist2\"]}', get_dataset('generic_metrics'))",
        "Query(\n        from_clause=Entity(EntityKey.GENERIC_METRICS_DISTRIBUTIONS,get_entity(EntityKey.GENERIC_METRICS_DISTRIBUTIONS).get_data_model()),\n        selected_columns=[SelectedExpression('aggregate_value', f.sum(column('value'), alias='aggregate_value'))],\n        array_join=None,\n        condition=and_cond(f.equals(column('metric_id'), literal('d:transactions/duration@millisecond')), in_cond(column('dist'), f.tuple(literal('dist1'), literal('dist2')))),\n        groupby=None,\n        having=None,\n        order_by=None,\n        limitby=None,\n        limit=None,\n        offset=0,\n        totals=False,\n        granularity=None,\n    )",
    ),
]
