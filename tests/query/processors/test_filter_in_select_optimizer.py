import pytest

from snuba.datasets.factory import get_dataset
from snuba.query.conditions import binary_condition
from snuba.query.expressions import (
    Column,
    Expression,
    FunctionCall,
    Literal,
    SubscriptableReference,
)
from snuba.query.logical import Query
from snuba.query.mql.parser import parse_mql_query
from snuba.query.processors.logical.filter_in_select_optimizer import (
    FilterInSelectOptimizer,
)

""" CONFIG STUFF THAT DOESNT MATTER MUCH """

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
        "d:transactions/duration@second": 123457,
        "status_code": 222222,
        "transaction": 333333,
    },
    "limit": None,
    "offset": None,
}
assert isinstance(
    mql_context["indexer_mappings"], dict
)  # oh mypy, my oh mypy, how you check types

""" TEST CASES """


def subscriptable_reference(name: str, key: str) -> SubscriptableReference:
    """Helper function to build a SubscriptableReference"""
    return SubscriptableReference(
        f"_snuba_{name}[{key}]",
        Column(f"_snuba_{name}", None, name),
        Literal(None, key),
    )


def equals(lhs: Expression, rhs: Expression) -> FunctionCall:
    return binary_condition("equals", lhs, rhs)


def _and(lhs: FunctionCall, rhs: FunctionCall) -> FunctionCall:
    return binary_condition("and", lhs, rhs)


def _or(lhs: FunctionCall, rhs: FunctionCall) -> FunctionCall:
    return binary_condition("or", lhs, rhs)


def _in(lhs: Expression, rhs: Expression) -> FunctionCall:
    return binary_condition("in", lhs, rhs)


new_mql_test_cases: list[tuple[str, FunctionCall]] = [
    (
        "sum(`d:transactions/duration@millisecond`){status_code:200} / sum(`d:transactions/duration@second`)",
        _or(
            equals(
                Column("_snuba_metric_id", None, "metric_id"),
                Literal(None, 123456),
            ),
            _and(
                equals(
                    subscriptable_reference(
                        "tags_raw", str(mql_context["indexer_mappings"]["status_code"])
                    ),
                    Literal(None, "200"),
                ),
                equals(
                    Column("_snuba_metric_id", None, "metric_id"),
                    Literal(None, 123456),
                ),
            ),
        ),
    ),
    (
        "sum(`d:transactions/duration@millisecond`){status_code:200} by transaction / sum(`d:transactions/duration@millisecond`) by transaction",
        _or(
            equals(
                Column("_snuba_metric_id", None, "metric_id"),
                Literal(None, 123456),
            ),
            _and(
                equals(
                    subscriptable_reference(
                        "tags_raw", str(mql_context["indexer_mappings"]["status_code"])
                    ),
                    Literal(None, "200"),
                ),
                equals(
                    Column("_snuba_metric_id", None, "metric_id"),
                    Literal(None, 123456),
                ),
            ),
        ),
    ),
    (
        "quantiles(0.5)(`d:transactions/duration@millisecond`){status_code:200} by transaction / sum(`d:transactions/duration@millisecond`) by transaction",
        _or(
            equals(
                Column("_snuba_metric_id", None, "metric_id"),
                Literal(None, 123456),
            ),
            _and(
                equals(
                    subscriptable_reference(
                        "tags_raw", str(mql_context["indexer_mappings"]["status_code"])
                    ),
                    Literal(None, "200"),
                ),
                equals(
                    Column("_snuba_metric_id", None, "metric_id"),
                    Literal(None, 123456),
                ),
            ),
        ),
    ),
    (
        "sum(`d:transactions/duration@millisecond`) / ((max(`d:transactions/duration@millisecond`) + avg(`d:transactions/duration@millisecond`)) * min(`d:transactions/duration@millisecond`))",
        _or(
            equals(
                Column("_snuba_metric_id", None, "metric_id"),
                Literal(None, 123456),
            ),
            _or(
                equals(
                    Column("_snuba_metric_id", None, "metric_id"),
                    Literal(None, 123456),
                ),
                _or(
                    equals(
                        Column("_snuba_metric_id", None, "metric_id"),
                        Literal(None, 123456),
                    ),
                    equals(
                        Column("_snuba_metric_id", None, "metric_id"),
                        Literal(None, 123456),
                    ),
                ),
            ),
        ),
    ),
    (
        "(sum(`d:transactions/duration@millisecond`) / max(`d:transactions/duration@millisecond`)){status_code:200}",
        _or(
            _and(
                equals(
                    subscriptable_reference(
                        "tags_raw", str(mql_context["indexer_mappings"]["status_code"])
                    ),
                    Literal(None, "200"),
                ),
                equals(
                    Column("_snuba_metric_id", None, "metric_id"),
                    Literal(None, 123456),
                ),
            ),
            _and(
                equals(
                    subscriptable_reference(
                        "tags_raw", str(mql_context["indexer_mappings"]["status_code"])
                    ),
                    Literal(None, "200"),
                ),
                equals(
                    Column("_snuba_metric_id", None, "metric_id"),
                    Literal(None, 123456),
                ),
            ),
        ),
    ),
    (
        "(sum(`d:transactions/duration@millisecond`) / max(`d:transactions/duration@millisecond`)){status_code:[400,404,500,501]}",
        _or(
            _and(
                _in(
                    subscriptable_reference(
                        "tags_raw", str(mql_context["indexer_mappings"]["status_code"])
                    ),
                    FunctionCall(
                        None,
                        "array",
                        (
                            Literal(None, "400"),
                            Literal(None, "404"),
                            Literal(None, "500"),
                            Literal(None, "501"),
                        ),
                    ),
                ),
                equals(
                    Column("_snuba_metric_id", None, "metric_id"),
                    Literal(None, 123456),
                ),
            ),
            _and(
                _in(
                    subscriptable_reference(
                        "tags_raw", str(mql_context["indexer_mappings"]["status_code"])
                    ),
                    FunctionCall(
                        None,
                        "array",
                        (
                            Literal(None, "400"),
                            Literal(None, "404"),
                            Literal(None, "500"),
                            Literal(None, "501"),
                        ),
                    ),
                ),
                equals(
                    Column("_snuba_metric_id", None, "metric_id"),
                    Literal(None, 123456),
                ),
            ),
        ),
    ),
    (
        "(sum(`d:transactions/duration@millisecond`) / max(`d:transactions/duration@millisecond`)){status_code:200} by transaction",
        _or(
            _and(
                equals(
                    subscriptable_reference(
                        "tags_raw", str(mql_context["indexer_mappings"]["status_code"])
                    ),
                    Literal(None, "200"),
                ),
                equals(
                    Column("_snuba_metric_id", None, "metric_id"),
                    Literal(None, 123456),
                ),
            ),
            _and(
                equals(
                    subscriptable_reference(
                        "tags_raw", str(mql_context["indexer_mappings"]["status_code"])
                    ),
                    Literal(None, "200"),
                ),
                equals(
                    Column("_snuba_metric_id", None, "metric_id"),
                    Literal(None, 123456),
                ),
            ),
        ),
    ),
    (
        "(sum(`d:transactions/duration@millisecond`) / sum(`d:transactions/duration@millisecond`)) + 100",
        _or(
            equals(
                Column("_snuba_metric_id", None, "metric_id"),
                Literal(None, 123456),
            ),
            equals(
                Column("_snuba_metric_id", None, "metric_id"),
                Literal(None, 123456),
            ),
        ),
    ),
    (
        "sum(`d:transactions/duration@millisecond`) * 1000",
        equals(
            Column("_snuba_metric_id", None, "metric_id"),
            Literal(None, 123456),
        ),
    ),
]

""" TESTING """


@pytest.mark.parametrize(
    "mql_query, expected_condition",
    new_mql_test_cases,
)
def test_condition_generation(mql_query: str, expected_condition: FunctionCall) -> None:
    logical_query, _ = parse_mql_query(str(mql_query), mql_context, generic_metrics)
    assert isinstance(logical_query, Query)

    opt = FilterInSelectOptimizer()
    actual = opt.get_select_filter(logical_query)
    if actual != expected_condition:
        assert actual != expected_condition
