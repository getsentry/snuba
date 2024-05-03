import re

import pytest

from snuba.datasets.dataset import Dataset
from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.factory import get_dataset
from snuba.query import SelectedExpression
from snuba.query.data_source.simple import Entity
from snuba.query.dsl import Functions as f
from snuba.query.dsl import and_cond, column, in_cond, literal
from snuba.query.expressions import CurriedFunctionCall
from snuba.query.logical import Query
from snuba.query.mql.parser import parse_mql_query_body
from snuba.query.parser.exceptions import ParsingException

test_cases = [
    pytest.param(
        "sum(`d:transactions/duration@millisecond`){status_code:200} / sum(`d:transactions/duration@millisecond`)",
        get_dataset("generic_metrics"),
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
    ),
    pytest.param(
        "1 + sum(`d:transactions/duration@millisecond`){status_code:200} / sum(`d:transactions/duration@millisecond`)",
        get_dataset("generic_metrics"),
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
    ),
    pytest.param(
        "sum(`d:transactions/duration@millisecond`){status_code:200} by transaction / sum(`d:transactions/duration@millisecond`) by transaction",
        get_dataset("generic_metrics"),
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
    ),
    pytest.param(
        "quantiles(0.5)(`d:transactions/duration@millisecond`){status_code:200} by transaction / sum(`d:transactions/duration@millisecond`) by transaction",
        get_dataset("generic_metrics"),
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
    ),
    pytest.param(
        "sum(`d:transactions/duration@millisecond`) / ((max(`d:transactions/duration@millisecond`) + avg(`d:transactions/duration@millisecond`)) * min(`d:transactions/duration@millisecond`))",
        get_dataset("generic_metrics"),
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
    ),
    pytest.param(
        "(sum(`d:transactions/duration@millisecond`) / max(`d:transactions/duration@millisecond`)){status_code:200}",
        get_dataset("generic_metrics"),
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
    ),
    pytest.param(
        "(sum(`d:transactions/duration@millisecond`) / max(`d:transactions/duration@millisecond`)){status_code:200} by transaction",
        get_dataset("generic_metrics"),
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
    ),
    pytest.param(
        "(sum(`d:transactions/duration@millisecond`) / sum(`d:transactions/duration@millisecond`)) + 100",
        get_dataset("generic_metrics"),
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
    ),
    pytest.param(
        "apdex(sum(`d:transactions/duration@millisecond`), 123) / max(`d:transactions/duration@millisecond`)",
        get_dataset("generic_metrics"),
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
    ),
    pytest.param(
        'sum(`d:transactions/duration@millisecond`){dist:["dist1", "dist2"]} by (transaction, status_code)',
        get_dataset("generic_metrics"),
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
    ),
    pytest.param(
        'sum(`d:transactions/duration@millisecond`){dist:["dist1", "dist2"]}',
        get_dataset("generic_metrics"),
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
    ),
    pytest.param(
        "sum(`d:transactions/duration@millisecond`){}",
        get_dataset("generic_metrics"),
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
    ),
    pytest.param(
        'quantiles(0.5, 0.75)(s:transactions/user@none{!dist:["dist1", "dist2"]}){foo: bar} by (transaction)',
        get_dataset("generic_metrics"),
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
    ),
    pytest.param(
        'quantiles(0.5)(`d:transactions/duration@millisecond`){dist:["dist1", "dist2"]} by (transaction, status_code)',
        get_dataset("generic_metrics"),
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
    ),
    pytest.param(
        'sum(`d:sessions/duration@second`){release:["foo", "bar"]} by release',
        get_dataset("metrics"),
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
    ),
    pytest.param(
        'max(d:transactions/duration@millisecond){bar:" !\\"#$%&\'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\\\]^_`abcdefghijklmnopqrstuvwxyz{|}~"} by (transaction)',
        get_dataset("generic_metrics"),
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
    ),
    pytest.param(
        'apdex(sum(`d:transactions/duration@millisecond`), 500){dist:["dist1", "dist2"]}',
        get_dataset("generic_metrics"),
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
    ),
    pytest.param(
        "topK(10)(sum(s:transactions/user@none), 300)",
        get_dataset("generic_metrics"),
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
    ),
    pytest.param(
        'avg(d:custom/sentry.event_manager.save_transactions.fetch_organizations@second){(event_type:"transaction" AND transaction:"sentry.tasks.store.save_event_transaction")}',
        get_dataset("generic_metrics"),
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
    ),
    pytest.param(
        'sum(`d:transactions/duration@millisecond`){dist:["dist1", "dist2"]}',
        get_dataset("generic_metrics"),
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
    ),
    pytest.param(
        'sum(`d:transactions/duration@millisecond`){dist:["dist1", "dist2"]}',
        get_dataset("generic_metrics"),
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
    ),
    pytest.param(
        'sum(`d:transactions/duration@millisecond`){dist:["dist1", "dist2"]}',
        get_dataset("generic_metrics"),
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
    ),
    pytest.param(
        'sum(`d:transactions/duration@millisecond`){dist:["dist1", "dist2"]}',
        get_dataset("generic_metrics"),
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
    ),
    pytest.param(
        '((avg(d:transactions/duration@millisecond) * 100.0) * 100.0){transaction:"getsentry.tasks.calculate_spike_projections"}',
        get_dataset("generic_metrics"),
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
    ),
]


@pytest.mark.parametrize("mql, dataset, expected", test_cases)
def test_autogenerated(mql: str, dataset: Dataset, expected: Query) -> None:
    actual = parse_mql_query_body(mql, dataset)
    assert actual == expected


def test_mismatch_groupby() -> None:
    query_body = "sum(`d:transactions/duration@millisecond`){status_code:200} by transaction / sum(`d:transactions/duration@millisecond`) by status_code"
    with pytest.raises(
        Exception,
        match=re.escape("All terms in a formula must have the same groupby"),
    ):
        parse_mql_query_body(str(query_body), get_dataset("generic_metrics"))


def test_invalid_mri() -> None:
    mql = 'sum(`transaction.duration`){dist:["dist1", "dist2"]}'
    expected = ParsingException("MQL endpoint only supports MRIs")
    with pytest.raises(type(expected), match=re.escape(str(expected))):
        parse_mql_query_body(mql, get_dataset("generic_metrics"))


def test_invalid_mql() -> None:
    mql = "sum(`transaction.duration"
    expected = ParsingException("Parsing error on line 1 at 'um(`transacti'")
    with pytest.raises(type(expected), match=re.escape(str(expected))):
        parse_mql_query_body(mql, get_dataset("generic_metrics"))


@pytest.mark.xfail(reason="Not implemented yet")
def test_apdex1() -> None:
    query_body = "apdex(sum(`d:transactions/duration@millisecond`) / max(`d:transactions/duration@millisecond`), 123)"
    parse_mql_query_body(query_body, get_dataset("generic_metrics"))


@pytest.mark.xfail(reason="Not implemented yet")
def test_apdex2() -> None:
    query_body = 'apdex(sum(`d:transactions/duration@millisecond`) / max(`d:transactions/duration@millisecond`), 500){dist:["dist1", "dist2"]}'
    parse_mql_query_body(query_body, get_dataset("generic_metrics"))
