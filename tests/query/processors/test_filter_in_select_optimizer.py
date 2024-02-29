import pytest

from snuba.datasets.factory import get_dataset
from snuba.query import SelectedExpression
from snuba.query.conditions import binary_condition
from snuba.query.expressions import Column, FunctionCall, Literal
from snuba.query.logical import Query
from snuba.query.mql.parser import parse_mql_query
from snuba.query.processors.logical.filter_in_select_optimizer import (
    FilterInSelectOptimizer,
)

""" CONFIG STUFF THAT DOESNT MATTER """

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

""" IMPORTANT STUFF """

optimizer = FilterInSelectOptimizer()
mql_test_cases: list[tuple[str, set[int] | None]] = []
logical_test_cases: list[tuple[Query, set[int] | None]] = []


@pytest.mark.parametrize(
    "mql_query, expected_domain",
    mql_test_cases,
)
def test_get_domain_of_mql(mql_query: str, expected_domain: set[int]) -> None:
    logical_query, _ = parse_mql_query(str(mql_query), mql_context, generic_metrics)
    assert isinstance(logical_query, Query)
    res = optimizer.get_domain_of_query(logical_query)
    assert res == expected_domain


@pytest.mark.parametrize("logical_query, expected_domain", logical_test_cases)
def test_get_domain_of_logical(logical_query: Query, expected_domain: set[int]):
    assert optimizer.get_domain_of_query(logical_query) == expected_domain


""" --- MQL TEST CASES --- """
mql_test_cases.append(
    (
        "quantiles(0.5)(`d:transactions/duration@millisecond`){status_code:200} by transaction / sum(`d:transactions/duration@millisecond`) by transaction",
        {123456},
    )
)
mql_test_cases.append(
    (
        "sum(`d:transactions/duration@millisecond`) / ((max(`d:transactions/duration@millisecond`) + avg(`d:transactions/duration@millisecond`)) * min(`d:transactions/duration@millisecond`))",
        {123456},
    )
)
mql_test_cases.append(
    (
        "(sum(`d:transactions/duration@millisecond`) / max(`d:transactions/duration@millisecond`)){status_code:200}",
        {123456},
    )
)
mql_test_cases.append(
    (
        "(sum(`d:transactions/duration@millisecond`) / sum(`d:transactions/duration@millisecond`)) + 100",
        {123456},
    )
)

""" --- LOGICAL TEST CASES --- """

basic_query_equals = Query(
    from_clause=None,
    selected_columns=[
        SelectedExpression(
            name=None,
            expression=FunctionCall(
                alias=None,
                function_name="sumIf",
                parameters=(
                    Column(None, None, "value"),
                    binary_condition(
                        "equals", Column(None, None, "metric_id"), Literal(None, 123456)
                    ),
                ),
            ),
        )
    ],
)
logical_test_cases.append((basic_query_equals, {123456}))

basic_query_in = Query(
    from_clause=None,
    selected_columns=[
        SelectedExpression(
            name=None,
            expression=FunctionCall(
                alias=None,
                function_name="sumIf",
                parameters=(
                    Column(None, None, "value"),
                    binary_condition(
                        "in",
                        Column(None, None, "metric_id"),
                        FunctionCall(
                            alias=None,
                            function_name="array",
                            parameters=(
                                Literal(None, 123456),
                                Literal(None, 987654),
                                Literal(None, 000000),
                            ),
                        ),
                    ),
                ),
            ),
        )
    ],
)
logical_test_cases.append((basic_query_in, {123456, 987654, 000000}))

basic_query_and = Query(
    from_clause=None,
    selected_columns=[
        SelectedExpression(
            name=None,
            expression=FunctionCall(
                alias=None,
                function_name="sumIf",
                parameters=(
                    Column(None, None, "value"),
                    binary_condition(
                        "and",
                        binary_condition(
                            "equals",
                            Column(None, None, "tag"),
                            Literal(None, "success"),
                        ),
                        binary_condition(
                            "equals",
                            Column(None, None, "metric_id"),
                            Literal(None, 123456),
                        ),
                    ),
                ),
            ),
        )
    ],
)
logical_test_cases.append((basic_query_and, {123456}))

query_with_multiple_selected = Query(
    from_clause=None,
    selected_columns=[
        SelectedExpression(
            name=None,
            expression=FunctionCall(
                alias=None,
                function_name="sumIf",
                parameters=(
                    Column(None, None, "value"),
                    binary_condition(
                        "equals", Column(None, None, "metric_id"), Literal(None, 123456)
                    ),
                ),
            ),
        ),
        SelectedExpression(name=None, expression=Column(None, None, "name")),
    ],
)
logical_test_cases.append((query_with_multiple_selected, {123456}))


cond_agg_in_divide = Query(
    from_clause=None,
    selected_columns=[
        SelectedExpression(
            name=None,
            expression=FunctionCall(
                alias=None,
                function_name="divide",
                parameters=(
                    FunctionCall(
                        alias=None,
                        function_name="sumIf",
                        parameters=(
                            Column(None, None, "value"),
                            binary_condition(
                                "equals",
                                Column(None, None, "metric_id"),
                                Literal(None, 123456),
                            ),
                        ),
                    ),
                    FunctionCall(
                        alias=None,
                        function_name="sumIf",
                        parameters=(
                            Column(None, None, "value"),
                            binary_condition(
                                "in",
                                Column(None, None, "metric_id"),
                                FunctionCall(
                                    alias=None,
                                    function_name="array",
                                    parameters=(
                                        Literal(None, 123456),
                                        Literal(None, 987654),
                                        Literal(None, 000000),
                                    ),
                                ),
                            ),
                        ),
                    ),
                ),
            ),
        )
    ],
)
logical_test_cases.append((cond_agg_in_divide, {123456, 987654, 000000}))

cond_agg_in_divide_in_add = Query(
    from_clause=None,
    selected_columns=[
        SelectedExpression(
            name=None,
            expression=FunctionCall(
                alias=None,
                function_name="add",
                parameters=(
                    cond_agg_in_divide.get_selected_columns()[0].expression,
                    FunctionCall(
                        alias=None,
                        function_name="sumIf",
                        parameters=(
                            Column(None, None, "value"),
                            binary_condition(
                                "equals",
                                Column(None, None, "metric_id"),
                                Literal(None, 888888),
                            ),
                        ),
                    ),
                ),
            ),
        )
    ],
)
logical_test_cases.append((cond_agg_in_divide_in_add, {123456, 987654, 000000, 888888}))

no_possible_filtering = Query(
    from_clause=None,
    selected_columns=[
        SelectedExpression(
            name=None,
            expression=FunctionCall(
                alias=None,
                function_name="add",
                parameters=(
                    cond_agg_in_divide.get_selected_columns()[0].expression,
                    FunctionCall(
                        alias=None,
                        function_name="sum",
                        parameters=(Column(None, None, "value"),),
                    ),
                ),
            ),
        )
    ],
)
logical_test_cases.append((no_possible_filtering, None))

unsupported_predicate_and_1 = Query(
    from_clause=None,
    selected_columns=[
        SelectedExpression(
            name=None,
            expression=FunctionCall(
                alias=None,
                function_name="sumIf",
                parameters=(
                    Column(None, None, "value"),
                    binary_condition(
                        "and",
                        binary_condition(
                            "and",
                            binary_condition(
                                "equals",
                                Column(None, None, "metric_id"),
                                Literal(None, 000000),
                            ),
                            binary_condition(
                                "equals",
                                Column(None, None, "metric_id"),
                                Literal(None, 888888),
                            ),
                        ),
                        binary_condition(
                            "equals",
                            Column(None, None, "metric_id"),
                            Literal(None, 123456),
                        ),
                    ),
                ),
            ),
        )
    ],
)
logical_test_cases.append((unsupported_predicate_and_1, None))

unsupported_predicate_and_2 = Query(
    from_clause=None,
    selected_columns=[
        SelectedExpression(
            name=None,
            expression=FunctionCall(
                alias=None,
                function_name="sumIf",
                parameters=(
                    Column(None, None, "value"),
                    binary_condition(
                        "and",
                        binary_condition(
                            "and",
                            binary_condition(
                                "equals",
                                Column(None, None, "metric_id"),
                                Literal(None, 000000),
                            ),
                            binary_condition(
                                "equals",
                                Column(None, None, "metric_id"),
                                Literal(None, 888888),
                            ),
                        ),
                        binary_condition(
                            "equals",
                            Column(None, None, "metric_id"),
                            Literal(None, 123456),
                        ),
                    ),
                ),
            ),
        )
    ],
)
logical_test_cases.append((unsupported_predicate_and_2, None))
