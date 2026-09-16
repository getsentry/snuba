from collections.abc import Callable

from sentry_protos.snuba.v1.endpoint_time_series_pb2 import (
    Expression as ProtoExpression,
)
from sentry_protos.snuba.v1.endpoint_trace_item_table_pb2 import (
    AggregationComparisonFilter,
    Column,
)

from snuba.query.dsl import Functions as f
from snuba.query.expressions import FunctionCall

COLUMN_OP_TO_EXPR: dict[int, Callable[..., FunctionCall]] = {
    Column.BinaryFormula.OP_ADD: f.plus,
    Column.BinaryFormula.OP_SUBTRACT: f.minus,
    Column.BinaryFormula.OP_MULTIPLY: f.multiply,
    Column.BinaryFormula.OP_DIVIDE: f.divide,
}

EXPRESSION_OP_TO_EXPR: dict[int, Callable[..., FunctionCall]] = {
    ProtoExpression.BinaryFormula.OP_ADD: f.plus,
    ProtoExpression.BinaryFormula.OP_SUBTRACT: f.minus,
    ProtoExpression.BinaryFormula.OP_MULTIPLY: f.multiply,
    ProtoExpression.BinaryFormula.OP_DIVIDE: f.divide,
}

FORMULA_CONDITION_OP_TO_EXPR: dict[int, Callable[..., FunctionCall]] = {
    Column.FormulaCondition.OP_LESS_THAN: f.less,
    Column.FormulaCondition.OP_GREATER_THAN: f.greater,
    Column.FormulaCondition.OP_LESS_THAN_OR_EQUALS: f.lessOrEquals,
    Column.FormulaCondition.OP_GREATER_THAN_OR_EQUALS: f.greaterOrEquals,
    Column.FormulaCondition.OP_EQUALS: f.equals,
    Column.FormulaCondition.OP_NOT_EQUALS: f.notEquals,
}

AGGREGATION_COMPARISON_OP_TO_EXPR: dict[int, Callable[..., FunctionCall]] = {
    AggregationComparisonFilter.OP_LESS_THAN: f.less,
    AggregationComparisonFilter.OP_GREATER_THAN: f.greater,
    AggregationComparisonFilter.OP_LESS_THAN_OR_EQUALS: f.lessOrEquals,
    AggregationComparisonFilter.OP_GREATER_THAN_OR_EQUALS: f.greaterOrEquals,
    AggregationComparisonFilter.OP_EQUALS: f.equals,
    AggregationComparisonFilter.OP_NOT_EQUALS: f.notEquals,
}
