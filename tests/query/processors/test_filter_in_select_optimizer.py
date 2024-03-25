from copy import copy

from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.query import SelectedExpression
from snuba.query.data_source.simple import Entity
from snuba.query.dsl import and_cond, divide, equals, multiply, or_cond, plus
from snuba.query.expressions import Column, CurriedFunctionCall, FunctionCall, Literal
from snuba.query.logical import Query
from snuba.query.processors.logical.filter_in_select_optimizer import (
    FilterInSelectOptimizer,
)
from snuba.query.query_settings import HTTPQuerySettings


def _equals(col_name: str, value: str | int) -> FunctionCall:
    return equals(Column(None, None, col_name), Literal(None, value))


def _cond_agg(function_name: str, condition: FunctionCall) -> FunctionCall:
    return FunctionCall(None, function_name, (Column(None, None, "value"), condition))


optimizer = FilterInSelectOptimizer()
from_entity = Entity(
    EntityKey.GENERIC_METRICS_DISTRIBUTIONS,
    get_entity(EntityKey.GENERIC_METRICS_DISTRIBUTIONS).get_data_model(),
)
settings = HTTPQuerySettings()


def test_simple_query() -> None:
    input_query = Query(
        from_clause=from_entity,
        selected_columns=[
            SelectedExpression(
                None,
                divide(
                    _cond_agg(
                        "sumIf",
                        and_cond(_equals("metric_id", 1), _equals("status_code", 200)),
                    ),
                    _cond_agg("sumIf", _equals("metric_id", 1)),
                ),
            )
        ],
    )
    expected_optimized_query = copy(input_query)
    expected_optimized_query.set_ast_condition(
        or_cond(
            and_cond(_equals("metric_id", 1), _equals("status_code", 200)),
            _equals("metric_id", 1),
        )
    )
    optimizer.process_query(input_query, settings)
    assert input_query == expected_optimized_query


def test_query_with_curried_function() -> None:
    input_query = Query(
        from_clause=from_entity,
        selected_columns=[
            SelectedExpression(
                None,
                divide(
                    CurriedFunctionCall(
                        alias=None,
                        internal_function=FunctionCall(
                            None, "quantilesIf", (Literal(None, 0.5),)
                        ),
                        parameters=(
                            Column(None, None, "value"),
                            and_cond(
                                _equals("metric_id", 1), _equals("status_code", 200)
                            ),
                        ),
                    ),
                    _cond_agg("sumIf", _equals("metric_id", 1)),
                ),
            )
        ],
    )
    expected_optimized_query = copy(input_query)
    expected_optimized_query.set_ast_condition(
        or_cond(
            and_cond(_equals("metric_id", 1), _equals("status_code", 200)),
            _equals("metric_id", 1),
        )
    )
    optimizer.process_query(input_query, settings)
    assert input_query == expected_optimized_query


def test_query_with_many_nested_functions() -> None:
    input_query = Query(
        from_clause=from_entity,
        selected_columns=[
            SelectedExpression(
                None,
                divide(
                    _cond_agg("sumIf", _equals("metric_id", 1)),
                    multiply(
                        plus(
                            _cond_agg("maxIf", _equals("metric_id", 1)),
                            _cond_agg("avgIf", _equals("metric_id", 1)),
                        ),
                        _cond_agg("minIf", _equals("metric_id", 1)),
                    ),
                ),
            )
        ],
    )
    expected_optimized_query = copy(input_query)
    expected_optimized_query.set_ast_condition(
        or_cond(
            or_cond(
                or_cond(_equals("metric_id", 1), _equals("metric_id", 1)),
                _equals("metric_id", 1),
            ),
            _equals("metric_id", 1),
        )
    )
    optimizer.process_query(input_query, settings)
    assert input_query == expected_optimized_query


def test_query_with_literal_arithmetic_in_select() -> None:
    input_query = Query(
        from_clause=from_entity,
        selected_columns=[
            SelectedExpression(
                None,
                plus(_cond_agg("sumIf", _equals("metric_id", 1)), Literal(None, 100.0)),
            )
        ],
    )
    expected_optimized_query = copy(input_query)
    expected_optimized_query.set_ast_condition(_equals("metric_id", 1))
    optimizer.process_query(input_query, settings)
    assert input_query == expected_optimized_query


def test_query_with_multiple_aggregate_columns() -> None:
    input_query = Query(
        from_clause=from_entity,
        selected_columns=[
            SelectedExpression(
                None,
                plus(_cond_agg("sumIf", _equals("metric_id", 1)), Literal(None, 100.0)),
            ),
            SelectedExpression(
                None,
                multiply(
                    _cond_agg("maxIf", _equals("metric_id", 2)),
                    _cond_agg("avgIf", _equals("metric_id", 2)),
                ),
            ),
        ],
    )
    expected_optimized_query = copy(input_query)
    expected_optimized_query.set_ast_condition(
        or_cond(
            or_cond(_equals("metric_id", 1), _equals("metric_id", 2)),
            _equals("metric_id", 2),
        )
    )
    optimizer.process_query(input_query, settings)
    assert input_query == expected_optimized_query
