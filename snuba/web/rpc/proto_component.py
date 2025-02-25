from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from sentry_protos.snuba.v1.attribute_conditional_aggregation_pb2 import (
    AttributeConditionalAggregation,
)
from sentry_protos.snuba.v1.endpoint_time_series_pb2 import (
    Expression as TimeSeriesExpression,
)
from sentry_protos.snuba.v1.endpoint_trace_item_table_pb2 import (
    AggregationComparisonFilter,
    Column,
)


class ProtoWrapper(ABC):
    def __init__(self, underlying_proto: Any):
        self.underlying_proto = underlying_proto

    @abstractmethod
    def accept(self, visitor: BaseVisitor) -> None:
        pass


class ColumnWrapper(ProtoWrapper):
    def accept(self, visitor: BaseVisitor) -> None:
        visitor.visit_column(self)


class AggregationFilterWrapper(ProtoWrapper):
    def accept(self, visitor: BaseVisitor) -> None:
        visitor.visit_aggregation_filter(self)


class TimeSeriesExpressionWrapper(ProtoWrapper):
    def accept(self, visitor: BaseVisitor) -> None:
        visitor.visit_time_series_expression(self)


class TraceItemTableRequestWrapper(ProtoWrapper):
    def accept(self, visitor: BaseVisitor) -> None:
        visitor.visit_trace_item_table_request(self)


class TimeSeriesRequestWrapper(ProtoWrapper):
    def accept(self, visitor: BaseVisitor) -> None:
        visitor.visit_time_series_request(self)


class OrderByWrapper(ProtoWrapper):
    def accept(self, visitor: BaseVisitor) -> None:
        visitor.visit_order_by(self)


class BaseVisitor:
    def visit_trace_item_table_request(self, field: ProtoWrapper) -> None:
        trace_item_table_request = field.underlying_proto
        for column in trace_item_table_request.columns:
            ColumnWrapper(column).accept(self)

        for ob in trace_item_table_request.order_by:
            OrderByWrapper(ob).accept(self)

        AggregationFilterWrapper(trace_item_table_request.aggregation_filter).accept(
            self
        )

    def visit_time_series_request(self, field: ProtoWrapper) -> None:
        time_series_request = field.underlying_proto
        for expression in time_series_request.expressions:
            TimeSeriesExpressionWrapper(expression).accept(self)

    def visit_order_by(self, field: ProtoWrapper) -> None:
        ColumnWrapper(field.underlying_proto.column).accept(self)

    def visit_column(self, field: ProtoWrapper) -> None:
        column = field.underlying_proto
        if column.HasField("formula"):
            if column.formula.HasField("left"):
                ColumnWrapper(column.formula.left).accept(self)
            if column.formula.HasField("right"):
                ColumnWrapper(column.formula.right).accept(self)

    def visit_aggregation_filter(self, field: ProtoWrapper) -> None:
        aggregation_filter = field.underlying_proto
        for agg_filter in aggregation_filter.and_filter.filters:
            AggregationFilterWrapper(agg_filter).accept(self)
        for agg_filter in aggregation_filter.or_filter.filters:
            AggregationFilterWrapper(agg_filter).accept(self)

    def visit_time_series_expression(self, field: ProtoWrapper) -> None:
        expression = field.underlying_proto
        if expression.HasField("formula"):
            TimeSeriesExpressionWrapper(expression.formula.left).accept(self)
            TimeSeriesExpressionWrapper(expression.formula.right).accept(self)

    def visit_time_series_expressions(self, field: ProtoWrapper) -> None:
        expressions = field.underlying_proto
        for expression in expressions:
            TimeSeriesExpressionWrapper(expression).accept(self)


def _convert_aggregation_to_conditional_aggregation(
    input: Column | AggregationComparisonFilter | TimeSeriesExpression,
) -> None:
    if input.HasField("aggregation"):
        aggregation = input.aggregation
        input.ClearField("aggregation")
        input.conditional_aggregation.CopyFrom(
            AttributeConditionalAggregation(
                aggregate=aggregation.aggregate,
                key=aggregation.key,
                label=aggregation.label,
                extrapolation_mode=aggregation.extrapolation_mode,
            )
        )


class AggregationToConditionalAggregationVisitor(BaseVisitor):
    """
    We support aggregation, but now we want to support conditional aggregation for the insights team, which only aggregates
    if the field satisfies the condition: https://clickhouse.com/docs/en/sql-reference/aggregate-functions/combinators#-if.
    For messages that don't have conditional aggregation, this function replaces the aggregation with a conditional aggregation,
    where the filter is null, and every field is the same. This allows code elsewhere to set the default condition to always
    be true.
    The reason we do this "transformation" is to avoid code fragmentation down the line, where we constantly have to check
    if the request contains `AttributeAggregation` or `AttributeConditionalAggregation`
    """

    def visit_column(self, field: ProtoWrapper) -> None:
        column = field.underlying_proto
        _convert_aggregation_to_conditional_aggregation(column)
        super().visit_column(field)

    def visit_aggregation_filter(self, field: ProtoWrapper) -> None:
        aggregation_filter = field.underlying_proto
        if aggregation_filter.HasField("comparison_filter"):
            _convert_aggregation_to_conditional_aggregation(
                aggregation_filter.comparison_filter
            )
        else:
            super().visit_aggregation_filter(field)

    def visit_time_series_expression(self, field: ProtoWrapper) -> None:
        time_series_expression = field.underlying_proto
        _convert_aggregation_to_conditional_aggregation(time_series_expression)
        super().visit_time_series_expression(field)
