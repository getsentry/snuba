import pytest

from snuba.datasets.factory import get_dataset
from snuba.query.expressions import Column, Literal, SubscriptableReference
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

""" TEST CASES """


def subscriptable_reference(name: str, key: str) -> SubscriptableReference:
    """Helper function to build a SubscriptableReference"""
    return SubscriptableReference(
        f"_snuba_{name}[{key}]",
        Column(f"_snuba_{name}", None, name),
        Literal(None, key),
    )


mql_test_cases: list[tuple[str, dict]] = [
    (
        "sum(`d:transactions/duration@millisecond`){status_code:200} / sum(`d:transactions/duration@millisecond`)",
        {
            Column("_snuba_metric_id", None, "metric_id"): {
                Literal(None, 123456),
            }
        },
    ),
    (
        "sum(`d:transactions/duration@millisecond`){status_code:200} / sum(`d:transactions/duration@second`)",
        {
            Column("_snuba_metric_id", None, "metric_id"): {
                Literal(None, 123456),
                Literal(None, 123457),
            }
        },
    ),
    (
        "sum(`d:transactions/duration@millisecond`){status_code:200} by transaction / sum(`d:transactions/duration@millisecond`) by transaction",
        {
            Column("_snuba_metric_id", None, "metric_id"): {
                Literal(None, 123456),
            }
        },
    ),
    (
        "quantiles(0.5)(`d:transactions/duration@millisecond`){status_code:200} by transaction / sum(`d:transactions/duration@millisecond`) by transaction",
        {
            Column("_snuba_metric_id", None, "metric_id"): {
                Literal(None, 123456),
            }
        },
    ),
    (
        "sum(`d:transactions/duration@millisecond`) / ((max(`d:transactions/duration@millisecond`) + avg(`d:transactions/duration@millisecond`)) * min(`d:transactions/duration@millisecond`))",
        {
            Column("_snuba_metric_id", None, "metric_id"): {
                Literal(None, 123456),
            }
        },
    ),
    (
        "(sum(`d:transactions/duration@millisecond`) / max(`d:transactions/duration@millisecond`)){status_code:200}",
        {
            Column("_snuba_metric_id", None, "metric_id"): {
                Literal(None, 123456),
            },
            subscriptable_reference("tags_raw", "222222"): {
                Literal(None, "200"),
            },
        },
    ),
    (
        "(sum(`d:transactions/duration@millisecond`) / max(`d:transactions/duration@millisecond`)){status_code:[400,404,500,501]}",
        {
            Column("_snuba_metric_id", None, "metric_id"): {
                Literal(None, 123456),
            },
            subscriptable_reference("tags_raw", "222222"): {
                Literal(None, "400"),
                Literal(None, "404"),
                Literal(None, "500"),
                Literal(None, "501"),
            },
        },
    ),
    (
        "(sum(`d:transactions/duration@millisecond`) / max(`d:transactions/duration@millisecond`)){status_code:200} by transaction",
        {
            Column("_snuba_metric_id", None, "metric_id"): {
                Literal(None, 123456),
            },
            subscriptable_reference("tags_raw", "222222"): {
                Literal(None, "200"),
            },
        },
    ),
    (
        "(sum(`d:transactions/duration@millisecond`) / sum(`d:transactions/duration@millisecond`)) + 100",
        {
            Column("_snuba_metric_id", None, "metric_id"): {
                Literal(None, 123456),
            },
        },
    ),
]

""" TESTING """

optimizer = FilterInSelectOptimizer()


@pytest.mark.parametrize(
    "mql_query, expected_domain",
    mql_test_cases,
)
def test_get_domain_of_mql(mql_query: str, expected_domain: set[int]) -> None:
    logical_query, _ = parse_mql_query(str(mql_query), mql_context, generic_metrics)
    assert isinstance(logical_query, Query)
    res = optimizer.get_domain_of_mql_query(logical_query)
    if res != expected_domain:
        raise
    assert res == expected_domain
